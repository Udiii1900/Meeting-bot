import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any

# ==========================================================
# ENV
# ==========================================================
# Stellt sicher, dass diese Umgebungsvariablen (Secrets) in GitHub gesetzt sind.
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
# HubSpot Owner ID zu Slack User Tag Mapping
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
def week_window(now: datetime):
    """Berechnet den Start (Montag 00:00) und das Ende der aktuellen Woche."""
    start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = start + timedelta(days=7)
    return start, end

# ==========================================================
# FETCH MEETINGS (CRM V3 APPOINTMENTS SEARCH)
# ==========================================================
def fetch_meetings(week_start: datetime, week_end: datetime) -> List[Dict[str, Any]]:
    """
    Ruft Meetings (Appointments) über die moderne HubSpot CRM V3 Search API ab.
    Mappt die V3-Daten zurück auf eine V1-ähnliche Struktur für die Kompatibilität mit MAIN.
    """
    url = "https://api.hubapi.com/crm/v3/objects/appointments/search"
    all_meetings = []
    after = None # Paginierungs-Token

    week_start_ms = int(week_start.timestamp() * 1000)
    week_end_ms = int(week_end.timestamp() * 1000)

    while True:
        body = {
            "properties": [
                "hs_timestamp", 
                "hubspot_owner_id",
                "hs_meeting_title"
            ],
            # KRITISCH: Verknüpfungen (Contacts) müssen explizit angefordert werden!
            "associations": ["contacts"], 
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "hs_timestamp",
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
                    "propertyName": "hs_timestamp",
                    "direction": "ASCENDING"
                }
            ]
        }

        r = requests.post(url, headers=HEADERS, json=body)
        r.raise_for_status()
        data = r.json()

        # Mapping der V3-Daten auf die erwartete V1-ähnliche Struktur
        for appointment in data.get("results", []):
            props = appointment.get("properties", {})
            associations = appointment.get("associations", {})

            v1_item = {
                "_start_ts": int(props["hs_timestamp"]), # Startzeitpunkt
                "engagement": {
                    "ownerId": props.get("hubspot_owner_id"),
                    "title": props.get("hs_meeting_title"),
                },
                "associations": {
                    "contactIds": [
                        assoc["id"] for assoc in associations.get("contacts", {}).get("results", [])
                    ]
                }
            }
            all_meetings.append(v1_item)

        # V3 Paginierungs-Logik
        if data.get("paging", {}).get("next"):
            after = data["paging"]["next"]["after"]
        else:
            break

    return all_meetings

# ==========================================================
# CONTACTS
# ==========================================================
def batch_read_contacts(contact_ids: List[str]) -> Dict[str, str]:
    """Ruft Kontaktdetails für eine Liste von IDs über die V3 Batch API ab."""
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
def build_message(grouped: Dict[str, List], week_start: datetime, week_end: datetime) -> str:
    """Formatiert die gruppierten Meetings in einer Slack-Nachricht."""
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

    # 1. Alle benötigten Kontakt-IDs sammeln
    all_contact_ids = set()
    for m in meetings:
        all_contact_ids.update(m.get("associations", {}).get("contactIds", []))

    # 2. Kontaktdetails abrufen
    contacts = batch_read_contacts(list(all_contact_ids))

    # 3. Meetings nach Owner gruppieren
    grouped = {}
    for m in meetings:
        eng = m["engagement"]
        owner = str(eng.get("ownerId"))
        
        # Meetings ohne Owner oder ohne assoziierte Kontakte überspringen
        cids = m.get("associations", {}).get("contactIds")
        if not owner or owner == "None" or not cids:
            continue

        # Daten extrahieren und formatieren
        dt = datetime.fromtimestamp(m["_start_ts"] / 1000, tz=TIMEZONE) 
        title = eng.get("title") or "Meeting"
        contact = contacts.get(cids[0], "Unbekannter Kontakt")

        grouped.setdefault(owner, []).append((dt, contact, title))

    # 4. Gruppierte Meetings sortieren (nach Datum/Uhrzeit)
    for o in grouped:
        grouped[o].sort(key=lambda x: x[0])

    # 5. Nachricht erstellen und senden
    message = build_message(grouped, week_start, week_end)
    r = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    r.raise_for_status()

if __name__ == "__main__":
    main()
