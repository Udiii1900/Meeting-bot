import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ==========================================================
# ENV
# ==========================================================
HUBSPOT_API_KEY = os.environ["HUBSPOT_API_KEY"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

TIMEZONE = ZoneInfo("Europe/Berlin")

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json",
}

OWNER_TO_SLACK = {
    "29202437": "<@U08N63C58BC>",
    "76287207": "<@U085X3R20P7>",
    "1331795909": "<@U07G8B29CN5>",
    "303586931": "<@U07K1NXC4TF>",
    "76160549": "<@U07M9L6U4SX>",
    "76822495": "<@U07FY6MUDEG>",
    "380546521": "<@U083BBL20BF>",
    "1859268659": "<@U07J82VKM9Q>",
    "982419171": "<@U07K4G7710B>",
    "78899599": "<@U08KDHHJ7S6>",
    "29454051": "<@U08TTADV078>",
    "1844730787": "<@U07JAJBKDLL>",
    "29545650": "<@U091QQP4W85>",
    "29700526": "<@U095R45NW8H>",
    "30562252": "<@U09LCQSB081>",
    "30767909": "<@U09PKAGQUF8>",
    "30840582": "<@U09QW1PVCCS>",
    "30287832": "<@U07M9P6JZ5G>",
    "31172664": "<@U0A0P2V70MC>",
    "30740680": "<@U09LSSAB3LH>",
}

WEEKDAY_DE = {
    0: "Montag", 1: "Dienstag", 2: "Mittwoch",
    3: "Donnerstag", 4: "Freitag", 5: "Samstag", 6: "Sonntag"
}

# ==========================================================
# WEEK WINDOW
# ==========================================================
def week_window(now):
    start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = start + timedelta(days=7)
    return start, end

# ==========================================================
# FETCH MEETINGS (ENGAGEMENTS)
# ==========================================================
def fetch_meetings(week_start, week_end):
    url = "https://api.hubapi.com/engagements/v1/engagements/paged"
    meetings = []
    offset = 0
    page = 0
    MAX_PAGES = 10

    week_start_ms = int(week_start.timestamp() * 1000)
    week_end_ms = int(week_end.timestamp() * 1000)

    while True:
        page += 1
        if page > MAX_PAGES:
            break

        r = requests.get(
            url,
            headers=HEADERS,
            params={"limit": 100, "offset": offset}
        )
        r.raise_for_status()
        data = r.json()

        for item in data.get("results", []):
            eng = item.get("engagement", {})
            meta = item.get("metadata", {})

            if eng.get("type") != "MEETING":
                continue

            # 🔑 DAS ist die echte Startzeit
            start_ts = meta.get("startTime") or eng.get("timestamp")
            if not start_ts:
                continue

            start_ts = int(start_ts)

            # 🔴 Abbruch sobald wir vor dieser Woche sind
            if start_ts < week_start_ms:
                return meetings

            if week_start_ms <= start_ts < week_end_ms:
                item["_start_ts"] = start_ts
                meetings.append(item)

        if not data.get("hasMore"):
            break

        offset = data.get("offset")

    return meetings

# ==========================================================
# CONTACTS
# ==========================================================
def batch_read_contacts(contact_ids):
    if not contact_ids:
        return {}

    r = requests.post(
        "https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
        headers=HEADERS,
        json={
            "properties": ["firstname", "lastname", "email"],
            "inputs": [{"id": cid} for cid in contact_ids],
        }
    )
    r.raise_for_status()

    out = {}
    for res in r.json().get("results", []):
        p = res.get("properties", {}) or {}
        name = " ".join(filter(None, [p.get("firstname"), p.get("lastname")]))
        out[res["id"]] = name or p.get("email") or f"Contact {res['id']}"
    return out

# ==========================================================
# BUILD SLACK MESSAGE
# ==========================================================
def build_message(grouped, week_start, week_end):
    ws = week_start.strftime("%d.%m.%Y")
    we = (week_end - timedelta(seconds=1)).strftime("%d.%m.%Y")

    if not grouped:
        return (
            f"📅 *Wochenübersicht – Meetings*\n"
            f"🗓️ Zeitraum: {ws} – {we}\n\n"
            f"✅ Diese Woche stehen keine anstehenden Meetings an."
        )

    lines = [
        "📅 *Wochenübersicht – Meetings*",
        f"🗓️ Zeitraum: {ws} – {we}\n"
    ]

    for owner_id, meetings in grouped.items():
        slack = OWNER_TO_SLACK.get(owner_id, f"<ID {owner_id}>")
        lines.append(f"*{slack}* hat diese Woche folgende anstehenden Meetings:")

        for dt, contact, title in meetings:
            lines.append(
                f"• {contact} | {title} | "
                f"{WEEKDAY_DE[dt.weekday()]}, {dt.strftime('%d.%m.%Y')}, {dt.strftime('%H:%M')}"
            )
        lines.append("")

    lines.append(
        "Solltet ihr noch offene Themen bei einem Kunden haben, "
        "die geklärt werden sollen, dann gebt bitte frühzeitig Bescheid."
    )

    return "\n".join(lines)

# ==========================================================
# MAIN
# ==========================================================
def main():
    now = datetime.now(TIMEZONE)
    week_start, week_end = week_window(now)

    meetings = fetch_meetings(week_start, week_end)

    all_contact_ids = set()
    for m in meetings:
        all_contact_ids.update(m.get("associations", {}).get("contactIds", []))

    contacts = batch_read_contacts(list(all_contact_ids))

    grouped = {}
    for m in meetings:
        eng = m["engagement"]
        owner = str(eng.get("ownerId"))
        if not owner:
            continue

        cids = m.get("associations", {}).get("contactIds")
        if not cids:
            continue

        dt = datetime.fromtimestamp(m["_start_ts"] / 1000, tz=TIMEZONE)
        title = eng.get("title") or "Meeting"
        contact = contacts.get(cids[0], "Unbekannter Kontakt")

        grouped.setdefault(owner, []).append((dt, contact, title))

    for o in grouped:
        grouped[o].sort(key=lambda x: x[0])

    r = requests.post(
        SLACK_WEBHOOK_URL,
        json={"text": build_message(grouped, week_start, week_end)}
    )
    r.raise_for_status()

if __name__ == "__main__":
    main()
