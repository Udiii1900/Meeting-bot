import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

HUBSPOT_API_KEY = os.environ["HUBSPOT_API_KEY"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

TIMEZONE = ZoneInfo("Europe/Berlin")

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json",
}

OWNER_TO_SLACK = {
    "29202437": "<@U08N63C58BC>",
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

def extract_start_ts(item):
    meta = item.get("metadata", {})
    eng = item.get("engagement", {})

    for key in ["startTimeMillis", "startTime"]:
        if key in meta:
            ts = int(meta[key])
            if ts < 10_000_000_000:
                ts *= 1000
            return ts

    ts = eng.get("timestamp")
    if ts:
        return int(ts)

    return None

def fetch_meetings():
    url = "https://api.hubapi.com/engagements/v1/engagements/paged"
    results = []
    offset = 0
    MAX_PAGES = 50  # 🔑 bewusst hoch

    for _ in range(MAX_PAGES):
        r = requests.get(
            url,
            headers=HEADERS,
            params={"limit": 100, "offset": offset}
        )
        r.raise_for_status()
        data = r.json()

        results.extend(data.get("results", []))

        if not data.get("hasMore"):
            break
        offset = data.get("offset")

    return results

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

    return "\n".join(lines)

def main():
    now = datetime.now(TIMEZONE)
    week_start, week_end = week_window(now)

    raw = fetch_meetings()

    meetings = []
    for item in raw:
        eng = item.get("engagement", {})
        if eng.get("type") != "MEETING":
            continue

        start_ts = extract_start_ts(item)
        if not start_ts:
            continue

        dt = datetime.fromtimestamp(start_ts / 1000, tz=TIMEZONE)
        if not (week_start <= dt < week_end):
            continue
        if dt < now:
            continue

        meetings.append((item, dt))

    contact_ids = set()
    for m, _ in meetings:
        contact_ids.update(m.get("associations", {}).get("contactIds", []))

    contacts = batch_read_contacts(list(contact_ids))

    grouped = {}
    for m, dt in meetings:
        eng = m["engagement"]
        owner = str(eng.get("ownerId"))
        cids = m.get("associations", {}).get("contactIds")
        if not owner or not cids:
            continue

        contact = contacts.get(cids[0], "Unbekannter Kontakt")
        title = eng.get("title") or "Meeting"

        grouped.setdefault(owner, []).append((dt, contact, title))

    msg = build_message(grouped, week_start, week_end)
    requests.post(SLACK_WEBHOOK_URL, json={"text": msg}).raise_for_status()

if __name__ == "__main__":
    main()
