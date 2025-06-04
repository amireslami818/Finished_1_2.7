# Sports Data Fetcher - Step 1 & 2.7

A robust, 24/7 sports data fetching system that retrieves live match data from TheSports API and processes it for further analysis.

## Features

- **24/7 Operation**: Continuous data fetching with automatic error recovery
- **Environment-based Configuration**: Secure credential management via `.env` file
- **Comprehensive Logging**: Clean, timestamped logs with New York timezone
- **Match Status Analysis**: Real-time tracking of In-Play matches (status IDs 2-7)
- **Service Management**: Easy start/stop/restart via shell script
- **Data Enrichment**: Fetches match details, odds, team info, and competition data

## Project Structure

```
├── json_fetch.py          # Main data fetcher (Step 1)
├── step2.7.py            # Data processor (Step 2.7)
├── start.sh              # Service management script
├── .env.template         # Environment variables template
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/amireslami818/Finished_1_2.7.git
   cd Finished_1_2.7
   ```

2. **Set up environment variables**:
   ```bash
   cp .env.template .env
   # Edit .env with your TheSports API credentials
   ```

3. **Make the start script executable**:
   ```bash
   chmod +x start.sh
   ```

## Usage

### Starting the Fetcher
```bash
./start.sh start
```

### Checking Status
```bash
./start.sh status
```

### Viewing Live Logs
```bash
./start.sh logs
```

### Stopping the Fetcher
```bash
./start.sh stop
```

### Restarting the Fetcher
```bash
./start.sh restart
```

## Data Flow

1. **Step 1 (json_fetch.py)**:
   - Fetches live matches from TheSports API
   - Enriches data with match details, odds, teams, and competitions
   - Logs comprehensive status summaries including In-Play match counts
   - Saves data to timestamped JSON files

2. **Step 2.7 (step2.7.py)**:
   - Processes the enriched data from Step 1
   - Additional analysis and formatting (implementation pending)

## Logging Format

The system uses clean, professional logging with:
- Single New York timestamp per fetch cycle (header/footer)
- No per-line timestamps for cleaner readability
- Comprehensive match status breakdowns
- **In-Play match counts** (status IDs 2-7) in each summary

### Sample Log Output
```
================================================================================
STEP 1 - DATA FETCH STARTED - 06/03/2025 07:56:22 PM EST
================================================================================
Fetching live matches from TheSports API...
✓ Live matches API call successful
  Response time: 0.27 seconds
  Total matches returned: 22
  Raw API status breakdown:
    First half (ID: 2): 2 matches
    Half-time (ID: 3): 7 matches
    Second half (ID: 4): 4 matches
    End (ID: 8): 6 matches
================================================================================
STEP 1 - FETCH COMPLETED SUCCESSFULLY - 06/03/2025 07:56:22 PM EST
Total execution time: 30.50 seconds
In-Play matches: 13 (status IDs 2-7)
================================================================================
```

## Match Status IDs

- **0**: Abnormal (suggest hiding)
- **1**: Not started
- **2-7**: **In-Play matches** (actively playing)
  - **2**: First half
  - **3**: Half-time
  - **4**: Second half
  - **5**: Overtime
  - **6**: Overtime (deprecated)
  - **7**: Penalty Shoot-out
- **8**: End
- **9**: Delay
- **10**: Interrupt
- **11**: Cut in half
- **12**: Cancel
- **13**: To be determined

## Requirements

- Python 3.8+
- aiohttp
- python-dotenv
- psutil

## Configuration

Set these environment variables in your `.env` file:
```
THESPORTS_USER=your_username
THESPORTS_SECRET=your_secret_key
```

## Monitoring

The system automatically:
- Creates virtual environment and installs dependencies
- Rotates log files daily
- Handles API errors and network issues
- Provides comprehensive status monitoring

## Contributing

This project is part of a sports data processing pipeline. Contributions and improvements are welcome.
