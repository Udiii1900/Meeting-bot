import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ==========================================================
# ENV VARS
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
# ZEITFENSTER
# ==========================================================
def week_window(now):
    start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = start + timedelta(days=7)
    return start, end

# ==========================================================
# TIMESTAMP NORMALISIEREN (SEK → MS)
# ==========================================================
def normalize_ts(value):
    ts = int(value)
    # alles < Jahr 2001 in ms ist Sekunden
    if ts < 10_000_000_000:
        ts *= 1000
    return ts

# ==========================================================
# APPOINTMENTS LISTEN
# ==========================================================
def fetch_appointments(week_start, week_end):
    url = "https://api.hubapi.com/crm/v3/objects/appointments"
    results = []
    after = None

    props = [
        "hs_start_time",
        "hs_timestamp",
        "hubspot_owner_id",
        "hs_owner_id",
        "hs_appointment_title",
        "hs_title",
    ]

    week_start_ms = int(week_start.timestamp() * 1000)
    week_end_ms = int(week_end.timestamp() * 1000)

    while True:
        params = {
            "limit": 100,
            "properties": ",".join(props),
        }
        if after:
            params["after"] = after

        r = requests.get(url, headers=HEADERS, params=params)
        r.raise_for_status()
        data = r.json()

        for item in data.get("results", []):
            p = item.get("properties", {}) or {}
            raw_ts = p.get("hs_start_time") or p.get("hs_timestamp")
            if not raw_ts:
                continue

            ts = normalize_ts(raw_ts)

            if week_start_ms <= ts < week_end_ms:
                item["_start_ts"] = ts
                results.append(item)

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return results

# ==========================================================
# CONTACTS
# ==========================================================
def fetch_contact_ids(appointment_id):
    url = f"https://api.hubapi.com/crm/v3/objects/appointments/{appointment_id}/associations/contacts"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return [x["id"] for x in r.json().get("results", [])]

def batch_read_contacts(contact_ids):
    if not contact_ids:
        return {}

    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/read"
    body = {
        "properties": ["firstname", "lastname", "email"],
        "inputs": [{"id": cid} for cid in contact_ids],
    }

    r = requests.post(url, headers=HEADERS, json=body)
    r.raise_for_status()

    out = {}
    for res in r.json().get("results", []):
        p = res.get("properties", {}) or {}
        name = " ".join(filter(None, [p.get("firstname"), p.get("lastname")]))
        out[res["id"]] = name or p.get("email") or f"Contact {res['id']}"
    return out

# ==========================================================
# SLACK MESSAGE
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
        f"🗓️ Zeitraum: {ws} – {we}\n",
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

    appointments = fetch_appointments(week_start, week_end)

    contact_map = {}
    all_contacts = set()

    for a in appointments:
        cids = fetch_contact_ids(a["id"])
        if cids:
            contact_map[a["id"]] = cids
            all_contacts.update(cids)

    contacts = batch_read_contacts(list(all_contacts))
    grouped = {}

    for a in appointments:
        props = a.get("properties", {}) or {}
        owner = props.get("hubspot_owner_id") or props.get("hs_owner_id")
        if not owner:
            continue

        cids = contact_map.get(a["id"])
        if not cids:
            continue

        dt = datetime.fromtimestamp(a["_start_ts"] / 1000, tz=TIMEZONE)
        title = props.get("hs_appointment_title") or props.get("hs_title") or "Meeting"
        contact = contacts.get(cids[0], "Unbekannter Kontakt")

        grouped.setdefault(owner, []).append((dt, contact, title))

    for o in grouped:
        grouped[o].sort(key=lambda x: x[0])

    msg = build_message(grouped, week_start, week_end)
    r = requests.post(SLACK_WEBHOOK_URL, json={"text": msg})
    r.raise_for_status()

if __name__ == "__main__":
    main()
