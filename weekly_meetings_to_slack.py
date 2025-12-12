import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import List, Dict, Any, Optional

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

# ==========================================================
# MAPPING
# ==========================================================
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
# HELFER: DATETIME ROBUST PARSEN
# ==========================================================
def parse_hubspot_datetime(value: Any) -> datetime:
    """
    Parst HubSpot Zeitstempel (ISO-String oder ms-Timestamp) in ein lokales datetime-Objekt.
    """
    if value is None or value == "":
        raise ValueError("Empty datetime value")

    # 1) Versuch: Zahl (Sekunden oder ms)
    try:
        num = int(value)
        if num < 10_000_000_000:  # Sekunden
            num *= 1000
        return datetime.fromtimestamp(num / 1000, tz=TIMEZONE)
    except (ValueError, TypeError):
        pass

    # 2) ISO-String
    iso = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(TIMEZONE)

# ==========================================================
# WOCHENFENSTER
# ==========================================================
def week_window(now: datetime) -> tuple[datetime, datetime]:
    start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = start + timedelta(days=7)
    return start, end

# ==========================================================
# MEETINGS (CRM SEARCH) - KORRIGIERT MIT ZEITFILTER
# ==========================================================
def fetch_meetings(week_start: datetime, week_end: datetime) -> List[Dict[str, Any]]:
    url = "https://api.hubapi.com/crm/v3/objects/meetings/search"
    all_meetings = []
    after = None

    # Zeitstempel in Millisekunden für den Filter
    week_start_ms = int(week_start.timestamp() * 1000)
    week_end_ms = int(week_end.timestamp() * 1000)

    while True:
        body = {
            "properties": [
                "hs_meeting_start_time",
                "hubspot_owner_id",
                "hs_meeting_title"
            ],
            "associations": ["contacts"],
            # 🔑 KRITISCH: Filterung nach Zeitstempel
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "hs_meeting_start_time",
                            "operator": "BETWEEN",
                            "value": week_start_ms,
                            "highValue": week_end_ms
                        }
                    ]
                }
            ],
            "limit": 100,
            "after": after,
            "sorts": [
                {
                    "propertyName": "hs_meeting_start_time",
                    "direction": "ASCENDING"
                }
            ]
        }

        r = requests.post(url, headers=HEADERS, json=body)
        r.raise_for_status()
        data = r.json()

        all_meetings.extend(data.get("results", []))

        if data.get("paging", {}).get("next"):
            after = data["paging"]["next"]["after"]
        else:
            break

        if after is None:
            break
    
    return all_meetings

# ==========================================================
# CONTACTS
# ==========================================================
def batch_read_contacts(contact_ids: List[str]) -> Dict[str, str]:
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

# ==========================================================
# SLACK MESSAGE
# ==========================================================
def build_message(grouped: Dict[str, List], week_start: datetime, week_end: datetime) -> str:
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

# ==========================================================
# MAIN - KORRIGIERT FÜR EFFIZIENTEN ABRUF
# ==========================================================
def main():
    now = datetime.now(TIMEZONE)
    week_start, week_end = week_window(now)

    # 🔑 Übergibt den Wochenfilter an die Fetch-Funktion
    meetings = fetch_meetings(week_start, week_end)

    grouped = defaultdict(list)
    contact_ids = set()

    # Kontakte sammeln und Meetings filtern
    for m in meetings:
        props = m.get("properties", {}) or {}
        owner = props.get("hubspot_owner_id")
        start_val = props.get("hs_meeting_start_time")

        # Zusätzliche lokale Filterung (z.B. nach abgelaufenen Meetings heute)
        try:
            dt = parse_hubspot_datetime(start_val)
        except ValueError:
            continue # Startzeit nicht parsbar

        if not owner or dt < now: # Nur zukünftige Meetings anzeigen
            continue

        assoc = m.get("associations", {}).get("contacts", {}).get("results", [])
        if not assoc:
            continue
        
        contact_ids.add(assoc[0]["id"])

    contacts = batch_read_contacts(list(contact_ids))

    for m in meetings:
        props = m.get("properties", {}) or {}
        owner = props.get("hubspot_owner_id")
        start_val = props.get("hs_meeting_start_time")
        
        # Erneute Prüfung nach lokalen Kriterien (Owner, Startzeit, Zukunft)
        try:
            dt = parse_hubspot_datetime(start_val)
        except ValueError:
            continue

        if not owner or dt < now:
            continue

        assoc = m.get("associations", {}).get("contacts", {}).get("results", [])
        if not assoc:
            continue

        contact = contacts.get(assoc[0]["id"], "Unbekannter Kontakt")
        title = props.get("hs_meeting_title") or "Meeting"

        grouped[str(owner)].append((dt, contact, title))


    # Gruppierte Meetings sortieren
    for o in grouped:
        grouped[o].sort(key=lambda x: x[0])

    msg = build_message(grouped, week_start, week_end)
    requests.post(SLACK_WEBHOOK_URL, json={"text": msg}).raise_for_status()

if __name__ == "__main__":
    main()
