#!/usr/bin/env python3
"""
STEP 1 – DATA FETCHER
--------------------
Fetches data from TheSports API and returns it as a dictionary.
No loops, no queues, no scheduling - runs once and exits.

IMPORTANT TERMINOLOGY:
- LIVE MATCHES = All matches from /match/detail_live API endpoint (broader category)
- IN-PLAY MATCHES = Only matches with status_id 2,3,4,5,6 (actively playing subset of live matches)
"""

import json
import logging
import logging.handlers
import os
import re
import requests
import shutil
import signal
import subprocess
import sys
import time
import traceback
import pytz
from datetime import datetime
from collections import defaultdict
from contextlib import contextmanager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging with rotating file handler (no timestamps on individual lines)
file_handler = logging.handlers.TimedRotatingFileHandler("step1.log", when="midnight", backupCount=7)
console_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",  # Remove timestamp from individual lines
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger(__name__)

# Get credentials from environment variables
USER = os.getenv("THESPORTS_USER", "thenecpt")  # Fallback for compatibility
SECRET = os.getenv("THESPORTS_SECRET", "0c55322e8e196d6ef9066fa4252cf386")  # Fallback for compatibility

# API base and endpoints
BASE_URL = "https://api.thesports.com/v1/football"
URLS = {
    "live":        f"{BASE_URL}/match/detail_live",
    "details":     f"{BASE_URL}/match/recent/list",
    "odds":        f"{BASE_URL}/odds/history",
    "team":        f"{BASE_URL}/team/additional/list",
    "competition": f"{BASE_URL}/competition/additional/list",
    "country":     f"{BASE_URL}/country/list",
}

def get_ny_time_str(format_str="%m/%d/%Y %I:%M:%S %p EST"):
    """Get current time in New York timezone with custom format"""
    ny_tz = pytz.timezone("America/New_York")
    return datetime.now(ny_tz).strftime(format_str)

def extract_status_id(match):
    """Extract status_id from match data, checking multiple locations"""
    return match.get("status_id") or (match.get("score", [None, None])[1] if isinstance(match.get("score"), list) and len(match.get("score", [])) > 1 else None)

def fetch_json(url: str, params: dict) -> dict:
    """Fetch JSON data with retry logic"""
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == 2:  # Last attempt
                logger.warning(f"API call failed for {url} after 3 attempts: {str(e)}")
                return {}
            time.sleep(2 ** attempt)  # Exponential backoff
    return {}

def fetch_live_matches():
    return fetch_json(URLS["live"], {"user": USER, "secret": SECRET})

def fetch_match_details(match_id):
    return fetch_json(URLS["details"], {"user": USER, "secret": SECRET, "uuid": match_id})

def fetch_match_odds(match_id):
    return fetch_json(URLS["odds"], {"user": USER, "secret": SECRET, "uuid": match_id})

def fetch_team_info(team_id):
    return fetch_json(URLS["team"], {"user": USER, "secret": SECRET, "uuid": team_id})

def fetch_competition_info(comp_id):
    return fetch_json(URLS["competition"], {"user": USER, "secret": SECRET, "uuid": comp_id})

def fetch_country_list():
    return fetch_json(URLS["country"], {"user": USER, "secret": SECRET})

def fetch_live_data():
    """Fetch live matches data"""
    logger.info("Fetching live matches from TheSports API...")
    api_start = datetime.now()
    
    live = fetch_live_matches()
    api_end = datetime.now()
    api_duration = (api_end - api_start).total_seconds()
    
    # Log API call success and basic info
    matches = live.get("results", [])
    total_matches = len(matches)
    
    logger.info(f"✓ Live matches API call successful")
    logger.info(f"  Response time: {api_duration:.2f} seconds")
    logger.info(f"  Total matches returned: {total_matches}")
    logger.info(f"  API response code: {live.get('code', 'Unknown')}")
    
    # Count status distribution from raw API response
    status_counts = {}
    matches_with_status = 0
    
    for match in matches:
        status_id = extract_status_id(match)
        if status_id is not None:
            status_counts[status_id] = status_counts.get(status_id, 0) + 1
            matches_with_status += 1
    
    logger.info(f"  Matches with status info: {matches_with_status}/{total_matches}")
    logger.info("  Raw API status breakdown:")
    
    # Log status breakdown with descriptions
    status_desc_map = {
        0: "Abnormal", 1: "Not started", 2: "First half", 3: "Half-time",
        4: "Second half", 5: "Overtime", 6: "Overtime (deprecated)",
        7: "Penalty Shoot-out", 8: "End", 9: "Delay", 10: "Interrupt",
        11: "Cut in half", 12: "Cancel", 13: "To be determined"
    }
    
    for status_id in sorted(status_counts.keys()):
        count = status_counts[status_id]
        desc = status_desc_map.get(status_id, f"Unknown Status")
        logger.info(f"    {desc} (ID: {status_id}): {count} matches")
    
    return live

def enrich_match_data(live_data, matches):
    """Enrich matches with detailed data (details, odds, teams, competitions)"""
    logger.info(f"Starting detailed data fetch for {len(matches)} matches...")
    detail_start = datetime.now()
    
    all_data = {
        "match_details": {},
        "match_odds": {},
        "team_info": {},
        "competition_info": {},
    }

    for match in matches:
        mid = match.get("id")

        # Add small delay to avoid rate limiting
        time.sleep(0.1)
        
        detail_wrap = fetch_match_details(mid)
        all_data["match_details"][mid] = detail_wrap

        all_data["match_odds"][mid] = fetch_match_odds(mid)

        detail = {}
        if isinstance(detail_wrap, dict):
            res = detail_wrap.get("results") or detail_wrap.get("result") or []
            if isinstance(res, list) and res:
                detail = res[0]

        home_id = detail.get("home_team_id") or match.get("home_team_id")
        away_id = detail.get("away_team_id") or match.get("away_team_id")
        comp_id = detail.get("competition_id") or match.get("competition_id")
        
        # Extract status_id from match details and add it to the main match object
        if detail.get("status_id") is not None:
            match["status_id"] = detail.get("status_id")

        if home_id and str(home_id) not in all_data["team_info"]:
            all_data["team_info"][str(home_id)] = fetch_team_info(home_id)
        if away_id and str(away_id) not in all_data["team_info"]:
            all_data["team_info"][str(away_id)] = fetch_team_info(away_id)
        if comp_id and str(comp_id) not in all_data["competition_info"]:
            all_data["competition_info"][str(comp_id)] = fetch_competition_info(comp_id)

    # Get countries data
    all_data["countries"] = fetch_country_list()
    
    # Log completion summary
    detail_end = datetime.now()
    detail_duration = (detail_end - detail_start).total_seconds()
    
    logger.info(f"Detailed data fetch time: {detail_duration:.2f} seconds")
    logger.info(f"Unique teams fetched: {len(all_data['team_info'])}")
    logger.info(f"Unique competitions fetched: {len(all_data['competition_info'])}")
    logger.info(f"Match details fetched: {len(all_data['match_details'])}")
    logger.info(f"Match odds fetched: {len(all_data['match_odds'])}")
    
    return all_data

def step1_main():
    """Fetch data once and return the data dict."""
    # Header with New York timestamp
    ny_time = get_ny_time_str()
    logger.info("="*80)
    logger.info(f"STEP 1 - DATA FETCH STARTED - {ny_time}")
    logger.info("="*80)
    
    start_time = datetime.now()
    
    # Fetch live matches
    live_data = fetch_live_data()
    matches = live_data.get("results", [])
    
    # Enrich with detailed data
    enriched_data = enrich_match_data(live_data, matches)
    
    # Combine all data
    all_data = {
        "timestamp": start_time.isoformat(),
        "live_matches": live_data,
        **enriched_data
    }
    
    # Footer with completion info
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()
    
    # Calculate In-Play match count for footer summary
    in_play_count = 0
    matches = live_data.get("results", [])
    for match in matches:
        status_id = extract_status_id(match)
        if status_id in [2, 3, 4, 5, 6, 7]:  # In-Play status IDs
            in_play_count += 1
    
    logger.info("="*80)
    logger.info(f"STEP 1 - FETCH COMPLETED SUCCESSFULLY - {ny_time}")
    logger.info(f"Total execution time: {total_duration:.2f} seconds")
    logger.info(f"In-Play matches: {in_play_count} (status IDs 2-7)")
    logger.info("="*80)

    return all_data

def save_to_json(data, filename):
    """Save data to a JSON file with pretty printing"""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Data saved to {filename}")

def get_ny_time():
    """Get current time in New York timezone - legacy function"""
    return get_ny_time_str('%m/%d/%Y %I:%M:%S %p')

def create_unified_status_summary(live_matches_data):
    """Unified function to create comprehensive status summary and counts"""
    if not live_matches_data or "results" not in live_matches_data:
        return {
            "total_matches_fetched": 0,
            "status_breakdown": {},
            "formatted_summary": [],
            "status_counts": {},
            "in_play_matches": 0
        }
    
    matches = live_matches_data["results"]
    total_matches = len(matches)
    
    # Official status mapping
    status_desc_map = {
        0: "Abnormal (suggest hiding)",
        1: "Not started", 
        2: "First half",
        3: "Half-time",
        4: "Second half",
        5: "Overtime",
        6: "Overtime (deprecated)",
        7: "Penalty Shoot-out",
        8: "End",
        9: "Delay",
        10: "Interrupt",
        11: "Cut in half",
        12: "Cancel",
        13: "To be determined"
    }
    
    # Count matches by status_id
    status_counts = {}
    matches_with_status = 0
    
    for match in matches:
        status_id = extract_status_id(match)
        if status_id is not None:
            matches_with_status += 1
            status_desc = status_desc_map.get(status_id, f"Unknown Status")
            if status_id not in status_counts:
                status_counts[status_id] = {
                    "description": status_desc,
                    "count": 0
                }
            status_counts[status_id]["count"] += 1
    
    # Create formatted summary lines and structured data
    formatted_summary = []
    status_counts_with_ids = {}
    
    for status_id in sorted(status_counts.keys()):
        data = status_counts[status_id]
        description = data["description"]
        count = data["count"]
        
        formatted_line = f"{description} (ID: {status_id}): {count}"
        formatted_summary.append(formatted_line)
        
        status_counts_with_ids[f"status_{status_id}"] = {
            "id": status_id,
            "description": description,
            "count": count,
            "formatted": formatted_line
        }
    
    # Calculate IN-PLAY matches (active statuses: 2,3,4,5,7)
    in_play_status_ids = [2, 3, 4, 5, 7]
    in_play_count = sum(status_counts.get(sid, {}).get("count", 0) for sid in in_play_status_ids)
    
    formatted_summary.append(f"IN-PLAY MATCHES: {in_play_count}")
    
    return {
        "total_matches_fetched": total_matches,
        "matches_with_status": matches_with_status,
        "matches_without_status": total_matches - matches_with_status,
        "in_play_matches": in_play_count,
        "status_breakdown": status_counts_with_ids,
        "formatted_summary": formatted_summary,
        "status_counts": {data["description"]: data["count"] for data in status_counts.values()}
    }

def create_detailed_status_mapping(live_matches_data):
    """Create detailed status mapping with match IDs for JSON output"""
    if not live_matches_data or "results" not in live_matches_data:
        return {}
    
    matches = live_matches_data["results"]
    
    # Official Status ID to description mapping
    status_desc_map = {
        0: "Abnormal (suggest hiding)",
        1: "Not started",
        2: "First half",
        3: "Half-time",
        4: "Second half",
        5: "Overtime",
        6: "Overtime (deprecated)",
        7: "Penalty Shoot-out",
        8: "End",
        9: "Delay",
        10: "Interrupt",
        11: "Cut in half",
        12: "Cancel",
        13: "To be determined"
    }
    
    # Group matches by status
    status_groups = {}
    for match in matches:
        match_id = match.get("id", "NO_ID")
        status_id = extract_status_id(match)
        
        if status_id is not None:
            status_desc = status_desc_map.get(status_id, f"Unknown Status (ID: {status_id})")
            
            if status_desc not in status_groups:
                status_groups[status_desc] = {
                    "status_id": status_id,
                    "count": 0,
                    "match_ids": []
                }
            
            status_groups[status_desc]["count"] += 1
            status_groups[status_desc]["match_ids"].append(match_id)
    
    return status_groups


def print_status_summary(live_matches_data):
    """Print and log a formatted summary of match counts by status"""
    summary_data = create_unified_status_summary(live_matches_data)
    
    if not summary_data["status_counts"]:
        message = "Step 1: No match data available for status summary"
        print(message)
        logger.info(message)
        return
    
    # Create the summary lines
    summary_lines = [
        "=" * 80,
        "                        STEP 1 - MATCH STATUS SUMMARY                        ",
        "=" * 80,
        f"Total Matches: {summary_data['total_matches_fetched']}",
        "-" * 80
    ]
    
    # Sort by count (descending) for better readability
    sorted_statuses = sorted(summary_data["status_counts"].items(), key=lambda x: x[1], reverse=True)
    
    for status, count in sorted_statuses:
        summary_lines.append(f"{status}: {count} Matches")
    
    summary_lines.append("=" * 80)
    
    # Print to console
    for line in summary_lines:
        print(line)
    
    # Log to file
    logger.info("STEP 1 - MATCH STATUS SUMMARY")
    logger.info(f"Total Matches: {summary_data['total_matches_fetched']}")
    for status, count in sorted_statuses:
        logger.info(f"{status}: {count} Matches")

def create_comprehensive_match_breakdown(all_data):
    """Create a comprehensive breakdown showing actual match details for each status"""
    if not all_data or "live_matches" not in all_data:
        return {}
    
    live_matches = all_data["live_matches"].get("results", [])
    match_details = all_data.get("match_details", {})
    team_info = all_data.get("team_info", {})
    
    # Status mapping
    status_desc_map = {
        0: "Abnormal", 1: "Not started", 2: "First half", 3: "Half-time",
        4: "Second half", 5: "Overtime", 6: "Overtime (deprecated)",
        7: "Penalty Shoot-out", 8: "End", 9: "Delay", 10: "Interrupt",
        11: "Cut in half", 12: "Cancel", 13: "To be determined"
    }
    
    # Group matches by status with full details
    status_breakdown = {}
    
    for match in live_matches:
        match_id = match.get("id", "NO_ID")
        status_id = extract_status_id(match)
        
        if status_id is not None:
            status_desc = status_desc_map.get(status_id, f"Unknown Status (ID: {status_id})")
            
            if status_desc not in status_breakdown:
                status_breakdown[status_desc] = {
                    "status_id": status_id,
                    "count": 0,
                    "matches": []
                }
            
            # Get match details
            match_detail = {}
            if match_id in match_details:
                detail_wrap = match_details[match_id]
                if isinstance(detail_wrap, dict):
                    results = detail_wrap.get("results") or detail_wrap.get("result") or []
                    if isinstance(results, list) and results:
                        match_detail = results[0]
            
            # Get team names
            home_team_name = "Unknown Home Team"
            away_team_name = "Unknown Away Team"
            
            home_team_id = match_detail.get("home_team_id") or match.get("home_team_id")
            away_team_id = match_detail.get("away_team_id") or match.get("away_team_id")
            
            if home_team_id and str(home_team_id) in team_info:
                team_data = team_info[str(home_team_id)]
                if isinstance(team_data, dict):
                    results = team_data.get("results") or team_data.get("result") or []
                    if isinstance(results, list) and results:
                        home_team_name = results[0].get("name", "Unknown Home Team")
            
            if away_team_id and str(away_team_id) in team_info:
                team_data = team_info[str(away_team_id)]
                if isinstance(team_data, dict):
                    results = team_data.get("results") or team_data.get("result") or []
                    if isinstance(results, list) and results:
                        away_team_name = results[0].get("name", "Unknown Away Team")
            
            # Get current score
            current_score = "0-0"
            if "score" in match and isinstance(match["score"], list) and len(match["score"]) >= 4:
                home_score = match["score"][2]
                away_score = match["score"][3]
                if isinstance(home_score, list) and isinstance(away_score, list):
                    # Get total score (usually index 0 is current score)
                    h_total = home_score[0] if home_score else 0
                    a_total = away_score[0] if away_score else 0
                    current_score = f"{h_total}-{a_total}"
            
            # Get competition name
            competition_name = "Unknown Competition"
            comp_id = match_detail.get("competition_id") or match.get("competition_id")
            if comp_id and str(comp_id) in all_data.get("competition_info", {}):
                comp_data = all_data["competition_info"][str(comp_id)]
                if isinstance(comp_data, dict):
                    results = comp_data.get("results") or comp_data.get("result") or []
                    if isinstance(results, list) and results:
                        competition_name = results[0].get("name", "Unknown Competition")
            
            # Create match summary
            match_summary = {
                "match_id": match_id,
                "home_team": home_team_name,
                "away_team": away_team_name,
                "score": current_score,
                "competition": competition_name,
                "formatted": f"{home_team_name} vs {away_team_name} ({current_score}) - {competition_name}"
            }
            
            status_breakdown[status_desc]["matches"].append(match_summary)
            status_breakdown[status_desc]["count"] += 1
    
    return status_breakdown


def print_comprehensive_match_breakdown(comprehensive_match_breakdown):
    """Print detailed match breakdown showing actual match info for each status"""
    print("\n" + "="*100)
    print("                    COMPREHENSIVE MATCH BREAKDOWN - ACTUAL MATCH DETAILS                    ")
    print("="*100)
    
    # Calculate IN-PLAY matches
    in_play_statuses = ["First half", "Half-time", "Second half", "Overtime", "Penalty Shoot-out"]
    total_in_play = 0
    
    for status_desc in sorted(comprehensive_match_breakdown.keys()):
        status_data = comprehensive_match_breakdown[status_desc]
        status_id = status_data["status_id"]
        count = status_data["count"]
        matches = status_data["matches"]
        
        if status_desc in in_play_statuses:
            total_in_play += count
        
        print(f"\n{status_desc.upper()} (ID: {status_id}) - {count} MATCHES:")
        print("-" * 100)
        
        for i, match in enumerate(matches[:5], 1):  # Show first 5 matches for each status
            print(f"  {i}. {match['formatted']}")
        
        if len(matches) > 5:
            print(f"  ... and {len(matches) - 5} more matches")
    
    print("\n" + "="*100)
    print(f"IN-PLAY MATCHES TOTAL: {total_in_play}")
    print("="*100)

if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Step 1 JSON Data Fetcher')
    parser.add_argument('--continuous', action='store_true', 
                       help='Run continuously in 60-second cycles (for production use)')
    args = parser.parse_args()

    try:
        if args.continuous:
            # Continuous mode - run every 60 seconds
            logger.info("Starting continuous mode (60-second cycles)...")
            while True:
                start_time = time.time()
                
                try:
                    result = step1_main()
                    
                    # Get match count for the console output
                    match_count = len(result.get('live_matches', {}).get('results', []))
                    print(f"Step 1: Fetched data with {match_count} matches")
                    
                    # Generate unified status summary (replaces multiple old functions)
                    unified_summary = create_unified_status_summary(result.get("live_matches", {}))
                    
                    # Create detailed status mapping
                    detailed_status_mapping = create_detailed_status_mapping(result.get("live_matches", {}))
                    
                    # Create comprehensive match breakdown with actual match details
                    comprehensive_match_breakdown = create_comprehensive_match_breakdown(result)
                    
                    # Add New York Eastern time timestamp to data
                    result["ny_timestamp"] = get_ny_time_str()
                    
                    # Add all status summaries to JSON
                    result["unified_status_summary"] = unified_summary
                    result["detailed_status_mapping"] = detailed_status_mapping
                    result["comprehensive_match_breakdown"] = comprehensive_match_breakdown
                    
                    # Always save to standard pipeline filename for compatibility (overwrites each time)
                    save_to_json(result, 'step1.json')
                    
                    # Daily rotation: Save with date-only filename at midnight NY time
                    ny_tz = pytz.timezone("America/New_York")
                    ny_now = datetime.now(ny_tz)
                    daily_filename = f'step1_{ny_now.strftime("%Y-%m-%d")}.json'
                    
                    # Only save daily file if it doesn't exist (once per day)
                    if not os.path.exists(daily_filename):
                        save_to_json(result, daily_filename)
                        logger.info(f"Daily rotation: Created {daily_filename}")
                    
                    # Print completion time in New York time
                    print(f"Data saved at {get_ny_time_str()} (New York Time)")
                    
                    # Print comprehensive summary with formatted breakdown
                    print("\n" + "="*80)
                    print("                    COMPREHENSIVE STATUS BREAKDOWN                    ")
                    print("="*80)
                    for line in unified_summary["formatted_summary"]:
                        print(line)
                    print(f"Total Matches Fetched: {unified_summary['total_matches_fetched']}")
                    print("="*80)
                    
                    # Print comprehensive match breakdown with actual match details (only once)
                    print_comprehensive_match_breakdown(comprehensive_match_breakdown)
                    
                except Exception as e:
                    logger.error(f"Error in cycle: {e}")
                    traceback.print_exc()
                
                # Sleep for remainder of 60 seconds
                duration = time.time() - start_time
                sleep_time = max(0, 60 - duration)
                logger.info(f"Cycle completed in {duration:.2f}s, sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
        else:
            # Single run mode - default behavior
            result = step1_main()
            
            # Get match count for the console output
            match_count = len(result.get('live_matches', {}).get('results', []))
            print(f"Step 1: Fetched data with {match_count} matches")
            
            # Generate unified status summary (replaces multiple old functions)
            unified_summary = create_unified_status_summary(result.get("live_matches", {}))
            
            # Create detailed status mapping
            detailed_status_mapping = create_detailed_status_mapping(result.get("live_matches", {}))
            
            # Create comprehensive match breakdown with actual match details
            comprehensive_match_breakdown = create_comprehensive_match_breakdown(result)
            
            # Add New York Eastern time timestamp to data
            result["ny_timestamp"] = get_ny_time_str()
            
            # Add all status summaries to JSON
            result["unified_status_summary"] = unified_summary
            result["detailed_status_mapping"] = detailed_status_mapping
            result["comprehensive_match_breakdown"] = comprehensive_match_breakdown
            
            # Always save to standard pipeline filename for compatibility (overwrites each time)
            save_to_json(result, 'step1.json')
            
            # Daily rotation: Save with date-only filename at midnight NY time
            ny_tz = pytz.timezone("America/New_York")
            ny_now = datetime.now(ny_tz)
            daily_filename = f'step1_{ny_now.strftime("%Y-%m-%d")}.json'
            
            # Only save daily file if it doesn't exist (once per day)
            if not os.path.exists(daily_filename):
                save_to_json(result, daily_filename)
                logger.info(f"Daily rotation: Created {daily_filename}")
            
            # Print completion time in New York time
            print(f"Data saved at {get_ny_time_str()} (New York Time)")
            
            # Print comprehensive summary with formatted breakdown
            print("\n" + "="*80)
            print("                    COMPREHENSIVE STATUS BREAKDOWN                    ")
            print("="*80)
            for line in unified_summary["formatted_summary"]:
                print(line)
            print(f"Total Matches Fetched: {unified_summary['total_matches_fetched']}")
            print("="*80)
            
            # Print comprehensive match breakdown with actual match details (only once)
            print_comprehensive_match_breakdown(comprehensive_match_breakdown)
        
    except KeyboardInterrupt:
        logger.info("Interrupted manually.")
    except Exception as e:
        logger.error(f"Error in execution: {e}")
        traceback.print_exc()