import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

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

def week_window(now):
    start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = start + timedelta(days=7)
    return start, end

def fetch_meetings(week_start, week_end):
    url = "https://api.hubapi.com/crm/v3/objects/meetings/search"

    start_ms = str(int(week_start.timestamp() * 1000))
    end_ms = str(int(week_end.timestamp() * 1000))

    body = {
        "properties": [
            "hs_meeting_start_time",
            "hubspot_owner_id",
            "hs_meeting_title"
        ],
        "associations": ["contacts"],
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "hs_meeting_start_time",
                        "operator": "BETWEEN",
                        "value": start_ms,
                        "highValue": end_ms
                    }
                ]
            }
        ],
        "sorts": [
            {
                "propertyName": "hs_meeting_start_time",
                "direction": "ASCENDING"
            }
        ],
        "limit": 100
    }

    r = requests.post(url, headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json().get("results", [])

def batch_read_contacts(contact_ids):
    if not contact_ids:
        return {}

    r = requests.post(
        "https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
        headers=HEADERS,
        json={
            "properties": ["firstname", "lastname", "email"],
            "inputs": [{"id": cid} for cid in contact_ids]
        }
    )
    r.raise_for_status()

    out = {}
    for res in r.json().get("results", []):
        p = res.get("properties", {}) or {}
        name = " ".join(filter(None, [p.get("firstname"), p.get("lastname")]))
        out[res["id"]] = name or p.get("email") or f"Contact {res['id']}"
    return out

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

    for owner, meetings in grouped.items():
        slack = OWNER_TO_SLACK.get(owner, f"<ID {owner}>")
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

def main():
    now = datetime.now(TIMEZONE)
    week_start, week_end = week_window(now)

    meetings = fetch_meetings(week_start, week_end)

    contact_ids = set()
    for m in meetings:
        for a in m.get("associations", {}).get("contacts", {}).get("results", []):
            contact_ids.add(a["id"])

    contacts = batch_read_contacts(list(contact_ids))
    grouped = defaultdict(list)

    for m in meetings:
        props = m.get("properties", {})
        owner = props.get("hubspot_owner_id")
        if not owner:
            continue

        start_ms = int(props["hs_meeting_start_time"])
        dt = datetime.fromtimestamp(start_ms / 1000, tz=TIMEZONE)

        assoc = m.get("associations", {}).get("contacts", {}).get("results", [])
        if not assoc:
            continue

        contact = contacts.get(assoc[0]["id"], "Unbekannter Kontakt")
        title = props.get("hs_meeting_title", "Meeting")

        grouped[str(owner)].append((dt, contact, title))

    msg = build_message(grouped, week_start, week_end)
    requests.post(SLACK_WEBHOOK_URL, json={"text": msg}).raise_for_status()

if __name__ == "__main__":
    main()
