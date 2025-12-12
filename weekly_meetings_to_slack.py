import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

# =========================
# ENV
# =========================
HUBSPOT_API_KEY = os.environ["HUBSPOT_API_KEY"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
DEBUG = os.environ.get("DEBUG", "0") == "1"

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

# =========================
# Helpers
# =========================
def post_to_slack(text: str):
    r = requests.post(SLACK_WEBHOOK_URL, json={"text": text})
    r.raise_for_status()

def week_window(now: datetime):
    start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=7)
    return start, end

def parse_hubspot_datetime(value) -> datetime:
    """
    HubSpot liefert Datetime-Properties portalabhängig als:
    - ISO8601 (z.B. 2025-12-08T10:00:00Z)
    - ms (z.B. 1765148400000)
    - seconds (z.B. 1765148400)
    """
    if value is None or value == "":
        raise ValueError("Empty datetime")

    # numeric?
    try:
        num = int(value)
        if num < 10_000_000_000:  # seconds -> ms
            num *= 1000
        return datetime.fromtimestamp(num / 1000, tz=TIMEZONE)
    except (ValueError, TypeError):
        pass

    # ISO
    iso = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(TIMEZONE)

# =========================
# HubSpot: Meetings
# =========================
def meetings_search(body: dict):
    url = "https://api.hubapi.com/crm/v3/objects/meetings/search"
    r = requests.post(url, headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json().get("results", [])

def fetch_meetings_candidates(week_start: datetime, week_end: datetime):
    """
    Robust:
    1) Search mit ms BETWEEN
    2) Falls 0 Ergebnisse: Search mit ISO BETWEEN
    3) Falls immer noch 0: unfiltered Search (limit 100), dann lokal filtern
    """
    props = ["hs_meeting_start_time", "hubspot_owner_id", "hs_meeting_title"]

    start_ms = str(int(week_start.timestamp() * 1000))
    end_ms = str(int(week_end.timestamp() * 1000))

    body_ms = {
        "properties": props,
        "filterGroups": [{
            "filters": [{
                "propertyName": "hs_meeting_start_time",
                "operator": "BETWEEN",
                "value": start_ms,
                "highValue": end_ms
            }]
        }],
        "sorts": [{"propertyName": "hs_meeting_start_time", "direction": "ASCENDING"}],
        "limit": 100
    }

    res = meetings_search(body_ms)
    if res:
        return res, "search_between_ms"

    # Attempt ISO range
    body_iso = {
        "properties": props,
        "filterGroups": [{
            "filters": [{
                "propertyName": "hs_meeting_start_time",
                "operator": "BETWEEN",
                "value": week_start.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z"),
                "highValue": week_end.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z"),
            }]
        }],
        "sorts": [{"propertyName": "hs_meeting_start_time", "direction": "ASCENDING"}],
        "limit": 100
    }

    res = meetings_search(body_iso)
    if res:
        return res, "search_between_iso"

    # Fallback: no filter, just get newest-ish and local filter
    body_any = {
        "properties": props,
        "sorts": [{"propertyName": "hs_meeting_start_time", "direction": "DESCENDING"}],
        "limit": 100
    }
    res = meetings_search(body_any)
    return res, "search_unfiltered_fallback"

def fetch_contact_ids_for_meeting(meeting_id: str):
    # v3 associations endpoint
    url = f"https://api.hubapi.com/crm/v3/objects/meetings/{meeting_id}/associations/contacts"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return [x["id"] for x in r.json().get("results", [])]

def batch_read_contacts(contact_ids):
    if not contact_ids:
        return {}

    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/read"
    r = requests.post(
        url,
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
        name = " ".join(filter(None, [p.get("firstname"), p.get("lastname")])).strip()
        out[res["id"]] = name or p.get("email") or f"Contact {res['id']}"
    return out

# =========================
# Slack message
# =========================
def build_message(grouped, week_start, week_end):
    ws = week_start.strftime("%d.%m.%Y")
    we = (week_end - timedelta(seconds=1)).strftime("%d.%m.%Y")

    if not grouped:
        return (
            f"📅 *Wochenübersicht – Meetings*\n"
            f"🗓️ Zeitraum: {ws} - {we}\n\n"
            f"✅ Diese Woche stehen keine anstehenden Meetings an."
        )

    lines = [
        "📅 *Wochenübersicht – Meetings*",
        f"🗓️ Zeitraum: {ws} - {we}\n",
    ]

    # sort owners by earliest meeting
    owners_sorted = sorted(grouped.keys(), key=lambda o: grouped[o][0][0])

    for owner in owners_sorted:
        slack = OWNER_TO_SLACK.get(owner, f"<ID {owner}>")
        lines.append(f"*{slack}* hat diese Woche folgende anstehenden Meetings:")
        for dt, contact, title in grouped[owner]:
            lines.append(
                f"• {contact} | {title} | {WEEKDAY_DE[dt.weekday()]}, {dt.strftime('%d.%m.%Y')}, {dt.strftime('%H:%M')}"
            )
        lines.append("")

    lines.append(
        "Solltet ihr noch offene Themen bei einem Kunden haben, die geklärt werden sollen, dann gebt bitte frühzeitig Bescheid."
    )
    return "\n".join(lines)

# =========================
# MAIN
# =========================
def main():
    now = datetime.now(TIMEZONE)
    week_start, week_end = week_window(now)

    meetings_raw, mode = fetch_meetings_candidates(week_start, week_end)

    # lokal filtern (auch wenn Search gefiltert hat, ist das safe)
    candidates = []
    parse_errors = 0

    for m in meetings_raw:
        props = m.get("properties", {}) or {}
        start_val = props.get("hs_meeting_start_time")
        owner = props.get("hubspot_owner_id")
        title = props.get("hs_meeting_title") or "Meeting"

        if not start_val or not owner:
            continue

        try:
            dt = parse_hubspot_datetime(start_val)
        except Exception:
            parse_errors += 1
            continue

        if not (week_start <= dt < week_end):
            continue
        if dt < now:
            continue

        candidates.append((m["id"], str(owner), dt, title, start_val))

    # jetzt Kontakte IMMER über Associations endpoint holen
    meeting_to_contact_ids = {}
    all_contact_ids = set()
    assoc_fail = 0

    for meeting_id, _, _, _, _ in candidates:
        try:
            cids = fetch_contact_ids_for_meeting(meeting_id)
        except Exception:
            assoc_fail += 1
            cids = []
        meeting_to_contact_ids[meeting_id] = cids
        all_contact_ids.update(cids)

    contacts = batch_read_contacts(list(all_contact_ids))

    grouped = defaultdict(list)
    no_contact = 0

    for meeting_id, owner, dt, title, _start_val in candidates:
        cids = meeting_to_contact_ids.get(meeting_id, [])
        if not cids:
            no_contact += 1
            continue
        contact_name = contacts.get(cids[0], f"Contact {cids[0]}")
        grouped[owner].append((dt, contact_name, title))

    for owner in grouped:
        grouped[owner].sort(key=lambda x: x[0])

    # Debug (einmalig super hilfreich)
    if DEBUG:
        sample = candidates[:3]
        sample_lines = "\n".join(
            [f"- id={mid}, owner={own}, start_raw={sv}, dt={dt.isoformat()}"
             for (mid, own, dt, _t, sv) in sample]
        ) or "- (keine candidates)"
        dbg = (
            "🧪 *DEBUG MeetingsBot*\n"
            f"Mode: `{mode}`\n"
            f"Meetings raw: {len(meetings_raw)}\n"
            f"Candidates (in week & future): {len(candidates)}\n"
            f"Parse errors: {parse_errors}\n"
            f"Assoc failures: {assoc_fail}\n"
            f"Candidates ohne Kontakte: {no_contact}\n"
            f"Owners mit Meetings: {len(grouped)}\n"
            f"Samples:\n{sample_lines}"
        )
        post_to_slack(dbg)

    msg = build_message(grouped, week_start, week_end)
    post_to_slack(msg)

if __name__ == "__main__":
    main()
