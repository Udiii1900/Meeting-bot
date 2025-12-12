import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ==========================================================
# ENV VARS (aus GitHub Secrets)
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
    "30740680": "<@U09LSSAB3LH>"
}

WEEKDAY_DE = {
    0: "Montag",
    1: "Dienstag",
    2: "Mittwoch",
    3: "Donnerstag",
    4: "Freitag",
    5: "Samstag",
    6: "Sonntag",
}

# ==========================================================
# ZEITFENSTER: diese Woche (Mo–So) + nur Zukunft
# ==========================================================
def week_window_ms(now):
    start_of_week = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_of_week = start_of_week + timedelta(days=7)

    start_ms = int(start_of_week.timestamp() * 1000)
    end_ms = int(end_of_week.timestamp() * 1000)
    now_ms = int(now.timestamp() * 1000)

    return max(start_ms, now_ms), end_ms, start_of_week, end_of_week


# ==========================================================
# APPOINTMENTS LISTEN (KEIN SEARCH!)
# ==========================================================
def fetch_appointments(from_ms, to_ms):
    url = "https://api.hubapi.com/crm/v3/objects/appointments"
    results = []
    after = None

    properties = [
        "hs_timestamp",
        "hs_start_time",
        "hubspot_owner_id",
        "hs_owner_id",
        "hs_title",
        "hs_appointment_title",
    ]

    while True:
        params = {
            "limit": 100,
            "properties": ",".join(properties),
        }
        if after:
            params["after"] = after

        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("results", []):
            props = item.get("properties", {}) or {}

            ts = props.get("hs_timestamp") or props.get("hs_start_time")
            if not ts:
                continue

            ts = int(ts)
            if from_ms <= ts < to_ms:
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
        slack_user = OWNER_TO_SLACK.get(owner_id, f"<ID {owner_id}>")
        lines.append(f"*{slack_user}* hat diese Woche folgende anstehenden Meetings:")

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
    from_ms, to_ms, week_start, week_end = week_window_ms(now)

    appointments = fetch_appointments(from_ms, to_ms)

    appointment_contacts = {}
    all_contact_ids = set()

    for a in appointments:
        cids = fetch_contact_ids(a["id"])
        if cids:
            appointment_contacts[a["id"]] = cids
            all_contact_ids.update(cids)

    contacts_map = batch_read_contacts(list(all_contact_ids))

    grouped = {}

    for a in appointments:
        props = a.get("properties", {}) or {}

        owner_id = props.get("hubspot_owner_id") or props.get("hs_owner_id")
        if not owner_id:
            continue

        ts = props.get("hs_timestamp") or props.get("hs_start_time")
        if not ts:
            continue

        cids = appointment_contacts.get(a["id"])
        if not cids:
            continue  # Meetings ohne Kontakt ignorieren

        contact_name = contacts_map.get(cids[0], "Unbekannter Kontakt")
        title = props.get("hs_appointment_title") or props.get("hs_title") or "Meeting"

        dt = datetime.fromtimestamp(int(ts) / 1000, tz=TIMEZONE)

        grouped.setdefault(owner_id, []).append((dt, contact_name, title))

    for owner_id in grouped:
        grouped[owner_id].sort(key=lambda x: x[0])

    message = build_message(grouped, week_start, week_end)
    r = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    r.raise_for_status()


if __name__ == "__main__":
    main()
