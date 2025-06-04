#!/usr/bin/env python3
"""
Combined Step 2 + Step 7 Module
This script merges match data from Step 1, extracts and summarizes fields (Step 2),
then filters in-play matches (Step 7) and prints/logs comprehensive summaries.
"""

import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import fcntl

# ---------------------------------------------------------------------------
# Constants and Path Configurations
# ---------------------------------------------------------------------------
TZ = ZoneInfo("America/New_York")
BASE_DIR = Path(__file__).resolve().parent
STEP1_JSON = BASE_DIR / "step1.json"
STATUS_FILTER = {2, 3, 4, 5, 6, 7}

# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------
def get_eastern_time() -> str:
    now = datetime.now(TZ)
    return now.strftime("%m/%d/%Y %I:%M:%S %p %Z")

# ---------------------------------------------------------------------------
# Step 2: Extraction & Summarization Helpers
# ---------------------------------------------------------------------------

def extract_summary_fields(match: dict) -> dict:
    """Return a compact summary structure for a single match."""
    home_live = home_ht = away_live = away_ht = 0
    sd = match.get("score", [])
    if isinstance(sd, list) and len(sd) > 3:
        hs, as_ = sd[2], sd[3]
        if isinstance(hs, list) and len(hs) > 1:
            home_live, home_ht = hs[0], hs[1]
        if isinstance(as_, list) and len(as_) > 1:
            away_live, away_ht = as_[0], as_[1]

    home_scores = match.get("home_scores", [])
    away_scores = match.get("away_scores", [])
    if home_scores and home_live == 0:
        home_live = home_scores[0] if isinstance(home_scores, list) and home_scores else 0
    if away_scores and away_live == 0:
        away_live = away_scores[0] if isinstance(away_scores, list) and away_scores else 0

    return {
        "match_id": match.get("match_id") or match.get("id"),
        "status": {
            "id": match.get("status_id"),
            "description": match.get("status", ""),
            "match_time": match.get("match_time", 0),
        },
        "teams": {
            "home": {
                "name": match.get("home_team", "Unknown"),
                "score": {"current": home_live, "halftime": home_ht, "detailed": home_scores},
                "position": match.get("home_position"),
                "country": match.get("home_country"),
                "logo_url": match.get("home_logo"),
            },
            "away": {
                "name": match.get("away_team", "Unknown"),
                "score": {"current": away_live, "halftime": away_ht, "detailed": away_scores},
                "position": match.get("away_position"),
                "country": match.get("away_country"),
                "logo_url": match.get("away_logo"),
            },
        },
        "competition": {
            "name": match.get("competition", "Unknown"),
            "id": match.get("competition_id"),
            "country": match.get("country"),
            "logo_url": match.get("competition_logo"),
        },
        "round": match.get("round", {}),
        "venue": match.get("venue_id"),
        "referee": match.get("referee_id"),
        "neutral": match.get("neutral") == 1,
        "coverage": match.get("coverage", {}),
        "start_time": match.get("scheduled"),
        "odds": extract_odds(match),
        "environment": extract_environment(match),
        "events": extract_events(match),
        "fetched_at": get_eastern_time(),
    }


def extract_odds(match: dict) -> dict:
    raw_odds = match.get("odds", {}) or {}
    data = {
        "full_time_result": {},
        "both_teams_to_score": {},
        "over_under": {},
        "spread": {},
        "raw": raw_odds
    }

    def _safe_minute(v):
        if v is None:
            return None
        m = re.match(r"(\d+)", str(v))
        return int(m.group(1)) if m else None

    def filter_by_time(entries):
        pts = [(_safe_minute(ent[1]), ent) for ent in entries if isinstance(ent, (list, tuple)) and len(ent) > 1]
        pts = [(m, e) for m, e in pts if m is not None]
        in_window = [e for m, e in pts if 3 <= m <= 6]
        if in_window:
            return in_window
        under_ten = [(m, e) for m, e in pts if m < 10]
        return [] if not under_ten else [min(under_ten, key=lambda t: abs(t[0] - 4.5))[1]]

    for key, idxs in [("eu", (2,3,4)), ("asia", (2,3,4)), ("bs", (2,3,4))]:
        entry = (filter_by_time(raw_odds.get(key, [])) or [None])[0]
        if entry and len(entry) >= max(idxs) + 1:
            if key == "eu":
                data["full_time_result"] = {
                    "home": entry[2], "draw": entry[3], "away": entry[4],
                    "timestamp": entry[0], "match_time": entry[1]
                }
            elif key == "asia":
                data["spread"] = {
                    "handicap": entry[3], "home": entry[2], "away": entry[4],
                    "timestamp": entry[0], "match_time": entry[1]
                }
            else:
                line = entry[3]
                data["over_under"][str(line)] = {
                    "line": line, "over": entry[2], "under": entry[4],
                    "timestamp": entry[0], "match_time": entry[1]
                }
                data["primary_over_under"] = data["over_under"][str(line)]

    for m in match.get("betting", {}).get("markets", []):
        if m.get("name") == "Both Teams to Score":
            for sel in m.get("selections", []):
                nm = sel.get("name", "").lower()
                if nm in ("yes", "no"):
                    data["both_teams_to_score"][nm] = sel.get("odds")
    return data


def extract_environment(match: dict) -> dict:
    env = match.get("environment", {}) or {}
    parsed = {"raw": env}
    wc = env.get("weather")
    parsed["weather"] = int(wc) if isinstance(wc, str) and wc.isdigit() else wc
    desc = {
        1: "Sunny", 2: "Partly Cloudy", 3: "Cloudy", 4: "Overcast",
        5: "Foggy", 6: "Light Rain", 7: "Rain", 8: "Heavy Rain",
        9: "Snow", 10: "Thunder"
    }
    parsed["weather_description"] = desc.get(parsed["weather"], "Unknown")

    for key in ("temperature", "wind", "pressure", "humidity"):
        val = env.get(key)
        parsed[key] = val
        m = re.match(r"([\d.-]+)\s*([^\d]*)", str(val))
        num, unit = (float(m.group(1)), m.group(2).strip()) if m else (None, None)
        parsed[f"{key}_value"] = num
        parsed[f"{key}_unit"] = unit

    wv = parsed.get("wind_value") or 0
    mph = wv * 2.237 if "m/s" in str(env.get("wind", "")).lower() else wv
    descs = [
        (1, "Calm"), (4, "Light Air"), (8, "Light Breeze"), (13, "Gentle Breeze"),
        (19, "Moderate Breeze"), (25, "Fresh Breeze"), (32, "Strong Breeze"),
        (39, "Near Gale"), (47, "Gale"), (55, "Strong Gale"), (64, "Storm"), (73, "Violent Storm")
    ]
    parsed["wind_description"] = next((label for lim, label in descs if mph < lim), "Hurricane")
    return parsed


def extract_events(match: dict) -> list:
    return [
        {"type": ev.get("type"), "time": ev.get("time"), "team": ev.get("team"),
         "player": ev.get("player"), "detail": ev.get("detail")}
        for ev in match.get("events", [])
        if ev.get("type") in {"goal", "yellowcard", "redcard", "penalty", "substitution"}
    ]


def save_match_summaries(summaries: list, output_file: str = "step2.json") -> bool:
    grouped = {str(s.get("match_id")): s for s in summaries if s.get("match_id")}
    batch = {
        "timestamp": datetime.now(TZ).isoformat(),
        "total_matches": len(grouped),
        "matches": grouped
    }
    path = os.path.join(os.path.dirname(__file__), output_file)
    try:
        data = {"history": []}
        if os.path.exists(path):
            with open(path, 'r') as f:
                loaded_data = json.load(f)
            data = loaded_data if isinstance(loaded_data, dict) and loaded_data.get("history") else {"history": [loaded_data]}

        MAX_HISTORY_ENTRIES = 100
        if len(data["history"]) >= MAX_HISTORY_ENTRIES:
            print(f"Step 2: Rotating history, keeping last {MAX_HISTORY_ENTRIES} entries")
            data["history"] = data["history"][-MAX_HISTORY_ENTRIES:]

        data["history"].append(batch)
        data.update({
            "last_updated": batch["timestamp"],
            "total_entries": len(data["history"]),
            "latest_match_count": batch["total_matches"]
        })
        data["ny_timestamp"] = get_eastern_time()

        with open(path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except:
        return False


def first_result(mapping: dict, key):
    wrap = mapping.get(str(key)) if key is not None else None
    if isinstance(wrap, dict):
        res = wrap.get("results") or wrap.get("result") or []
        return res[0] if isinstance(res, list) and res else {}
    return {}


def merge_and_summarize(live: dict, payload: dict) -> dict:
    mid = live.get("id") or live.get("match_id")
    dm, om, tm, cm = (
        payload.get("match_details", {}),
        payload.get("match_odds", {}),
        payload.get("team_info", {}),
        payload.get("competition_info", {})
    )
    cw = payload.get("countries", {})
    cl = cw.get("results") or cw.get("result") or []
    countries = {c.get("id"): c.get("name") for c in cl if isinstance(c, dict)}
    detail = first_result(dm, mid)
    odds_wrap = om.get(mid, {})
    odds_struct = {mt: od for mk in odds_wrap.get("results", {}).values() for mt, od in mk.items() if isinstance(mk, dict)}
    home = first_result(tm, live.get("home_team_id") or detail.get("home_team_id"))
    away = first_result(tm, live.get("away_team_id") or detail.get("away_team_id"))
    comp = first_result(cm, live.get("competition_id") or detail.get("competition_id"))

    merged = {**live, **detail,
              "odds": odds_struct,
              "environment": detail.get("environment", live.get("environment", {})),
              "events": detail.get("events", live.get("events", [])),
              "home_team": home.get("name") or live.get("home_name"),
              "home_logo": home.get("logo"),
              "home_country": home.get("country") or countries.get(home.get("country_id")),
              "away_team": away.get("name") or live.get("away_name"),
              "away_logo": away.get("logo"),
              "away_country": away.get("country") or countries.get(away.get("country_id")),
              "competition": comp.get("name") or live.get("competition_name"),
              "competition_logo": comp.get("logo"),
              "country": comp.get("country") or countries.get(comp.get("country_id")),
              "odds_raw": odds_wrap
    }
    return extract_summary_fields(merged)

async def extract_merge_summarize(data: dict):
    print("Step 2: Starting extract_merge_summarize...")
    matches = (data.get("live_matches", {}).get("results") or data.get("live_matches", {}).get("matches") or [])
    print(f"Step 2: Found {len(matches)} matches to process")
    summaries = [merge_and_summarize(m, data) for m in matches]
    print(f"Step 2: Created {len(summaries)} summaries")
    if summaries:
        if save_match_summaries(summaries):
            print(f"Step 2 produced {len(summaries)} summaries and saved to step2.json")
        else:
            print(f"Step 2 produced {len(summaries)} summaries but failed to save JSON file")
    else:
        print("Step 2 processed payload but found no matches to summarize")
    print("Step 2: Processing completed")
    return summaries

# ---------------------------------------------------------------------------
# Step 7: Status Filtering & Display Helpers
# ---------------------------------------------------------------------------

def setup_logger() -> logging.Logger:
    log_file = BASE_DIR / "step7_matches.log"
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger('step7_matches')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

match_logger = setup_logger()


def log_and_print(message: str):
    print(message)
    match_logger.info(message)
    for handler in match_logger.handlers:
        handler.flush()


def get_daily_fetch_count() -> int:
    counter_file = BASE_DIR / "step6" / "daily_fetch_counter.txt"
    try:
        if counter_file.exists():
            content = counter_file.read_text().strip()
            return int(content) if content else 1
        return 1
    except:
        return 1


def get_status_description(status_id: int) -> str:
    status_map = {
        0: "Abnormal (suggest hiding)",
        1: "Not started", 2: "First half", 3: "Half-time", 4: "Second half", 5: "Overtime", 6: "Overtime (deprecated)", 7: "Penalty Shoot-out", 8: "End", 9: "Delay", 10: "Interrupt", 11: "Cut in half", 12: "Cancel", 13: "To be determined"
    }
    return status_map.get(status_id, f"Unknown ({status_id})")


def sort_matches_by_competition_and_time(matches: dict) -> dict:
    competition_groups = {}
    for match_id, match_data in matches.items():
        comp = match_data.get("competition", "Unknown Competition")
        country = match_data.get("country", "Unknown") or "Unknown"
        if country in [None, "None"]:
            country = infer_country_from_teams(match_data)
        if comp not in competition_groups:
            competition_groups[comp] = {'country': country, 'matches': []}
        competition_groups[comp]['matches'].append((match_id, match_data))
    status_order = {2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6}
    for comp in competition_groups:
        competition_groups[comp]['matches'].sort(key=lambda item: (
            status_order.get(item[1].get("status_id", 99), 99), item[1].get("match_id", "")
        ))
    sorted_comps = sorted(
        competition_groups.items(),
        key=lambda item: (item[1]['country'] or "Unknown", item[0])
    )
    result = {comp: data['matches'] for comp, data in sorted_comps}
    return result


def write_main_header(fetch_count: int, total: int, generated_at: str, pipeline_time=None):
    header = (
        f"\n{'='*80}\n"
        f"🔥 STEP 7: STATUS FILTER (2–7)\n"
        f"{'='*80}\n"
        f"Filter Time: {get_eastern_time()}\n"
        f"Data Generated: {generated_at}\n"
        f"Pipeline Time: {pipeline_time or 'Not provided'}\n"
        f"Daily Fetch: #{fetch_count}\n"
        f"Statuses Filtered: {sorted(STATUS_FILTER)}\n"
        f"Included Matches Count: {total}\n"
        f"{'='*80}\n"
    )
    match_logger.info(header)
    for h in match_logger.handlers:
        h.flush()


def write_main_footer(fetch_count: int, total: int, generated_at: str, pipeline_time=None, matches=None):
    footer = (
        f"\n{'='*80}\n"
        f"END OF STATUS FILTER – STEP 7\n"
        f"{'='*80}\n"
        f"Summary Time: {get_eastern_time()}\n"
        f"Total Matches (statuses 2–7): {total}\n"
        f"Daily Fetch: #{fetch_count}\n"
        f"{'='*80}\n"
    )
    match_logger.info(footer)
    if matches and total > 0:
        status_counts = {}
        for match_data in matches.values():
            status_id = match_data.get("status_id")
            if status_id in STATUS_FILTER:
                status_counts[status_id] = status_counts.get(status_id, 0) + 1
        summary_footer = (
            f"\nSTEP 7 - STATUS SUMMARY\n"
            f"{'='*60}\n"
        )
        for status_id in sorted(status_counts.keys()):
            count = status_counts[status_id]
            desc = get_status_description(status_id)
            summary_footer += f"{desc} (ID: {status_id}): {count}\n"
        summary_footer += f"Total: {total}\n" f"{'='*60}\n"
        match_logger.info(summary_footer)
    for h in match_logger.handlers:
        h.flush()


def write_competition_group_header(competition: str, country: str, match_count: int):
    comp_line = f"🏆 {competition.upper()}"
    info_line = f"📍 {country} | 📊 {match_count} Matches"
    header = (
        f"\n{'='*100}\n"
        f"{'='*100}\n"
        f"{comp_line.center(100)}\n"
        f"{info_line.center(100)}\n"
        f"{'='*100}\n"
        f"{'='*100}\n"
    )
    log_and_print(header)


def format_american_odds(odds_value):
    if not odds_value or odds_value == 0:
        return "N/A"
    try:
        if isinstance(odds_value, str):
            if odds_value.startswith(("+", "-")):
                return odds_value
            try:
                num_val = float(odds_value)
                return f"+{int(num_val)}" if num_val > 0 else str(int(num_val))
            except ValueError:
                return odds_value
        odds_num = float(odds_value)
        return f"+{int(odds_num)}" if odds_num > 0 else str(int(odds_num))
    except:
        return str(odds_value) if odds_value else "N/A"


def format_betting_odds(match_data: dict) -> str:
    odds_lines = []
    full_time = match_data.get("full_time_result")
    if full_time and isinstance(full_time, dict):
        home_ml = format_american_odds(full_time.get('home'))
        draw_ml = format_american_odds(full_time.get('draw'))
        away_ml = format_american_odds(full_time.get('away'))
        time_stamp = full_time.get('time', '0')
        odds_lines.append(f"│ ML:     │ Home: {home_ml:>6} │ Draw: {draw_ml:>6} │ Away: {away_ml:>7} │ (@{time_stamp}')")
    spread = match_data.get("spread")
    if spread and isinstance(spread, dict):
        home_spread = format_american_odds(spread.get('home'))
        handicap = spread.get('handicap', 0)
        away_spread = format_american_odds(spread.get('away'))
        time_stamp = spread.get('time', '0')
        odds_lines.append(f"│ Spread: │ Home: {home_spread:>6} │ Hcap: {handicap:>6} │ Away: {away_spread:>7} │ (@{time_stamp}')")
    over_under = match_data.get("over_under")
    if over_under and isinstance(over_under, dict):
        for line_value, line_data in over_under.items():
            if isinstance(line_data, dict):
                over_odds = format_american_odds(line_data.get('over'))
                line_num = line_data.get('line', line_value)
                under_odds = format_american_odds(line_data.get('under'))
                time_stamp = line_data.get('time', '0')
                odds_lines.append(f"│ O/U:    │ Over: {over_odds:>6} │ Line: {line_num:>6} │ Under: {under_odds:>6} │ (@{time_stamp}')")
                break
    if not odds_lines:
        return "No betting odds available"
    return "\n".join(odds_lines)


def format_environment_data(match_data: dict) -> str:
    environment = match_data.get("environment", {})
    if not environment:
        return "No environment data available"
    weather = environment.get("weather_description", "Unknown")
    temp_c = environment.get("temperature_value", 0)
    temp_unit = environment.get("temperature_unit") or "°C"
    wind_desc = environment.get("wind_description", "Unknown")
    wind_value = environment.get("wind_value", 0)
    wind_unit = environment.get("wind_unit") or "m/s"
    if temp_unit == "°C" and temp_c:
        temp_f = (temp_c * 9/5) + 32
        temp_display = f"{temp_f:.1f}°F"
    else:
        temp_display = f"{temp_c}°{temp_unit.replace('°', '') if temp_unit else 'C'}"
    if wind_unit == "m/s" and wind_value:
        wind_mph = wind_value * 2.237
        wind_display = f"{wind_desc}, {wind_mph:.1f} mph"
    else:
        wind_display = f"{wind_desc}, {wind_value} {wind_unit}"
    return f"Weather: {weather}\nTemperature: {temp_display}\nWind: {wind_display}"


def infer_country_from_teams(match_data):
    home_team = match_data.get('home_team', '').lower()
    away_team = match_data.get('away_team', '').lower()
    competition = match_data.get('competition', '').lower()
    country_indicators = {
        'australia': ['australia', 'aussie', 'socceroos', 'matildas'],
        'argentina': ['argentina', 'boca', 'river plate', 'racing club'],
        'brazil': ['brazil', 'sao paulo', 'flamengo', 'corinthians', 'palmeiras'],
        'england': ['england', 'manchester', 'liverpool', 'chelsea', 'arsenal', 'tottenham'],
        'spain': ['spain', 'real madrid', 'barcelona', 'atletico', 'sevilla', 'valencia'],
        'germany': ['germany', 'bayern', 'borussia', 'schalke', 'hamburg'],
        'france': ['france', 'psg', 'marseille', 'lyon', 'monaco', 'saint-etienne'],
        'italy': ['italy', 'juventus', 'inter', 'milan', 'roma', 'napoli', 'lazio'],
        'netherlands': ['netherlands', 'ajax', 'psv', 'feyenoord'],
        'portugal': ['portugal', 'porto', 'benfica', 'sporting'],
        'mexico': ['mexico', 'america', 'guadalajara', 'cruz azul', 'pumas'],
        'usa': ['usa', 'united states', 'la galaxy', 'seattle sounders', 'new york'],
        'south korea': ['korea', 'seoul', 'busan', 'daegu'],
        'japan': ['japan', 'tokyo', 'osaka', 'yokohama', 'kashima'],
        'china': ['china', 'beijing', 'shanghai', 'guangzhou'],
        'russia': ['russia', 'moscow', 'spartak', 'cska', 'dynamo', 'zenit'],
        'norway': ['norway', 'oslo', 'bergen'],
        'czech republic': ['czech', 'praha', 'prague', 'brno'],
        'austria': ['austria', 'vienna', 'salzburg']
    }
    if 'international' in competition and 'friendly' in competition:
        home_countries, away_countries = [], []
        for country, indicators in country_indicators.items():
            for indicator in indicators:
                if indicator in home_team:
                    home_countries.append(country)
                if indicator in away_team:
                    away_countries.append(country)
        if home_countries and away_countries and home_countries[0] != away_countries[0]:
            return "International"
        if home_countries and not away_countries:
            return home_countries[0].title()
        if away_countries and not home_countries:
            return away_countries[0].title()
        if home_countries and away_countries and home_countries[0] == away_countries[0]:
            return home_countries[0].title()
    team_text = f"{home_team} {away_team}"
    for country, indicators in country_indicators.items():
        for indicator in indicators:
            if indicator in team_text:
                return country.title()
    return "Unknown"


def process_environment_like_step2(env):
    import re
    parsed = {"raw": env}
    wc = env.get("weather")
    parsed["weather"] = int(wc) if isinstance(wc, str) and wc.isdigit() else wc
    desc = {1: "Sunny", 2: "Partly Cloudy", 3: "Cloudy", 4: "Overcast", 5: "Foggy", 6: "Light Rain", 7: "Rain", 8: "Heavy Rain", 9: "Snow", 10: "Thunder"}
    parsed["weather_description"] = desc.get(parsed["weather"], f"Unknown ({parsed['weather']})")
    temp = env.get("temperature")
    if temp and str(temp).replace("-", "").isdigit():
        temp_c = int(temp)
        temp_f = round((temp_c * 9/5) + 32)
        parsed["temperature"] = f"{temp_c}°C ({temp_f}°F)"
    else:
        parsed["temperature"] = str(temp) if temp else "Unknown"
    wind = env.get("wind")
    if wind and str(wind).replace(".", "").isdigit():
        wind_mph = float(wind)
        if wind_mph < 1:
            parsed["wind_description"] = "Calm"
        elif wind_mph < 4:
            parsed["wind_description"] = "Light Air"
        elif wind_mph < 8:
            parsed["wind_description"] = "Light Breeze"
        elif wind_mph < 13:
            parsed["wind_description"] = "Gentle Breeze"
        elif wind_mph < 19:
            parsed["wind_description"] = "Moderate Breeze"
        elif wind_mph < 25:
            parsed["wind_description"] = "Fresh Breeze"
        elif wind_mph < 32:
            parsed["wind_description"] = "Strong Breeze"
        elif wind_mph < 39:
            parsed["wind_description"] = "Near Gale"
        elif wind_mph < 47:
            parsed["wind_description"] = "Gale"
        elif wind_mph < 55:
            parsed["wind_description"] = "Strong Gale"
        elif wind_mph < 64:
            parsed["wind_description"] = "Storm"
        elif wind_mph < 73:
            parsed["wind_description"] = "Violent Storm"
        else:
            parsed["wind_description"] = "Hurricane"
        parsed["wind"] = f"{wind_mph} mph ({parsed['wind_description']})"
    else:
        parsed["wind"] = str(wind) if wind else "Unknown"
    parsed["pressure"] = f"{env.get('pressure', 'Unknown')} hPa"
    parsed["humidity"] = f"{env.get('humidity', 'Unknown')}%"
    return parsed


def summarize_environment_step5_style(env_data):
    if not env_data:
        return "No environment data available"
    parts = []
    if "weather_description" in env_data:
        parts.append(env_data["weather_description"])
    if "temperature" in env_data:
        parts.append(env_data["temperature"])
    if "wind_description" in env_data:
        parts.append(f"Wind: {env_data['wind_description']}")
    return ", ".join(parts) if parts else "Environment data available"


def filter_odds_by_time(odds_entries):
    import re
    def _safe_minute(v):
        if v is None:
            return None
        m = re.match(r"(\d+)", str(v))
        return int(m.group(1)) if m else None
    pts = [(_safe_minute(ent[1]), ent) for ent in odds_entries if isinstance(ent, (list, tuple)) and len(ent) > 1]
    pts = [(m, e) for m, e in pts if m is not None]
    in_window = [e for m, e in pts if 3 <= m <= 6]
    if in_window:
        return in_window
    under_ten = [(m, e) for m, e in pts if m < 10]
    if under_ten:
        return [min(under_ten, key=lambda t: abs(t[0] - 4.5))[1]]
    return []


def process_betting_odds(filtered_odds):
    betting_odds = {}
    for odds_entry in filtered_odds:
        if isinstance(odds_entry, (list, tuple)) and len(odds_entry) > 5:
            odds_type = odds_entry[0]
            if odds_type == "1x2":  # Full Time Result
                betting_odds["full_time_result"] = {
                    "home": odds_entry[2] if len(odds_entry) > 2 else None,
                    "draw": odds_entry[3] if len(odds_entry) > 3 else None,
                    "away": odds_entry[4] if len(odds_entry) > 4 else None,
                    "time": odds_entry[1] if len(odds_entry) > 1 else "0"
                }
            elif odds_type == "ah":  # Asian Handicap (Spread)
                betting_odds["spread"] = {
                    "handicap": odds_entry[5] if len(odds_entry) > 5 else None,
                    "home": odds_entry[2] if len(odds_entry) > 2 else None,
                    "away": odds_entry[3] if len(odds_entry) > 3 else None,
                    "time": odds_entry[1] if len(odds_entry) > 1 else "0"
                }
            elif odds_type == "ou":  # Over/Under
                line = odds_entry[5] if len(odds_entry) > 5 else "0"
                if line not in betting_odds.get("over_under", {}):
                    if "over_under" not in betting_odds:
                        betting_odds["over_under"] = {}
                    betting_odds["over_under"][line] = {
                        "over": odds_entry[2] if len(odds_entry) > 2 else None,
                        "line": line,
                        "under": odds_entry[4] if len(odds_entry) > 4 else None
                    }
