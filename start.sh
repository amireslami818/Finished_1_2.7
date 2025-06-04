#!/bin/bash

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/json_fetch.py"
PID_FILE="$SCRIPT_DIR/json_fetch.pid"
LOG_FILE="$SCRIPT_DIR/start.log"
VENV_DIR="$SCRIPT_DIR/venv"
PYTHON_BIN="$VENV_DIR/bin/python"
PIP_BIN="$VENV_DIR/bin/pip"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

# Function to ensure virtual environment and dependencies
ensure_venv() {
    # Check if virtual environment exists
    if [ ! -d "$VENV_DIR" ]; then
        print_status "Creating Python virtual environment..."
        python3 -m venv "$VENV_DIR"
        if [ $? -ne 0 ]; then
            print_error "Failed to create virtual environment"
            return 1
        fi
    fi
    
    # Check if Python binary exists in venv
    if [ ! -f "$PYTHON_BIN" ]; then
        print_error "Virtual environment Python binary not found: $PYTHON_BIN"
        return 1
    fi
    
    # Install/upgrade required packages
    print_status "Checking and installing required packages..."
    
    # List of required packages
    REQUIRED_PACKAGES=("aiohttp" "python-dotenv" "psutil")
    
    for package in "${REQUIRED_PACKAGES[@]}"; do
        if ! "$PYTHON_BIN" -c "import $package" 2>/dev/null; then
            print_status "Installing $package..."
            "$PIP_BIN" install "$package" >> "$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                print_error "Failed to install $package"
                return 1
            fi
        fi
    done
    
    print_success "Virtual environment and dependencies are ready"
    return 0
}

# Function to check if the process is running
is_running() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            return 0
        else
            # PID file exists but process is not running, clean up
            rm -f "$PID_FILE"
            return 1
        fi
    else
        return 1
    fi
}

# Function to start the data fetcher
start_fetcher() {
    print_status "Starting JSON Data Fetcher..."
    
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_warning "Data fetcher is already running with PID: $PID"
        return 1
    fi
    
    # Check if Python script exists
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        print_error "Python script not found: $PYTHON_SCRIPT"
        return 1
    fi
    
    # Ensure virtual environment and dependencies
    if ! ensure_venv; then
        print_error "Failed to set up virtual environment"
        return 1
    fi
    
    # Start the Python script in background using virtual environment
    cd "$SCRIPT_DIR"
    nohup "$PYTHON_BIN" "$PYTHON_SCRIPT" >> "$LOG_FILE" 2>&1 &
    PID=$!
    
    # Save PID to file
    echo $PID > "$PID_FILE"
    
    # Wait a moment to see if it started successfully
    sleep 2
    
    if is_running; then
        print_success "✓ Data fetcher started successfully with PID: $PID"
        print_status "Log file: $LOG_FILE"
        print_status "Virtual environment: $VENV_DIR"
        print_status "Use './start.sh status' to check status"
        print_status "Use './start.sh stop' to stop the fetcher"
        print_status "Use './start.sh logs' to view live logs"
        return 0
    else
        print_error "✗ Failed to start data fetcher"
        rm -f "$PID_FILE"
        return 1
    fi
}

# Function to stop the data fetcher
stop_fetcher() {
    print_status "Stopping JSON Data Fetcher..."
    
    if ! is_running; then
        print_warning "Data fetcher is not running"
        return 1
    fi
    
    PID=$(cat "$PID_FILE")
    print_status "Sending SIGTERM to process $PID..."
    
    # Send SIGTERM first (graceful shutdown)
    if kill -TERM "$PID" 2>/dev/null; then
        # Wait up to 10 seconds for graceful shutdown
        for i in {1..10}; do
            if ! kill -0 "$PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        
        # If still running, force kill
        if kill -0 "$PID" 2>/dev/null; then
            print_warning "Process still running, sending SIGKILL..."
            kill -KILL "$PID" 2>/dev/null
            sleep 1
        fi
    fi
    
    # Clean up PID file
    rm -f "$PID_FILE"
    
    if ! kill -0 "$PID" 2>/dev/null; then
        print_success "✓ Data fetcher stopped successfully"
        return 0
    else
        print_error "✗ Failed to stop data fetcher"
        return 1
    fi
}

# Function to restart the data fetcher
restart_fetcher() {
    print_status "Restarting JSON Data Fetcher..."
    stop_fetcher
    sleep 2
    start_fetcher
}

# Function to show status
show_status() {
    print_status "JSON Data Fetcher Status:"
    echo "=========================="
    
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_success "Status: RUNNING (PID: $PID)"
        
        # Show process details
        if command -v ps >/dev/null 2>&1; then
            echo -e "\n${BLUE}Process Details:${NC}"
            ps -p "$PID" -o pid,ppid,pcpu,pmem,etime,cmd 2>/dev/null || echo "Process details unavailable"
        fi
        
        # Show recent log entries
        if [ -f "$LOG_FILE" ]; then
            echo -e "\n${BLUE}Recent Log Entries (last 5 lines):${NC}"
            tail -n 5 "$LOG_FILE"
        fi
        
        # Show JSON files
        echo -e "\n${BLUE}Generated Files:${NC}"
        ls -la "$SCRIPT_DIR"/*.json 2>/dev/null | head -5 || echo "No JSON files found"
        
    else
        print_error "Status: NOT RUNNING"
    fi
    
    echo -e "\n${BLUE}Virtual Environment:${NC}"
    if [ -d "$VENV_DIR" ]; then
        echo "  Virtual env: $VENV_DIR (✓ exists)"
        echo "  Python path: $PYTHON_BIN"
        if [ -f "$PYTHON_BIN" ]; then
            PYTHON_VERSION=$("$PYTHON_BIN" --version 2>&1)
            echo "  Python version: $PYTHON_VERSION"
        fi
    else
        echo "  Virtual env: Not created"
    fi
    
    echo -e "\n${BLUE}Available Commands:${NC}"
    echo "  ./start.sh start   - Start the data fetcher"
    echo "  ./start.sh stop    - Stop the data fetcher"
    echo "  ./start.sh restart - Restart the data fetcher"
    echo "  ./start.sh status  - Show this status"
    echo "  ./start.sh logs    - Show live logs"
}

# Function to show live logs
show_logs() {
    if [ ! -f "$LOG_FILE" ]; then
        print_error "Log file not found: $LOG_FILE"
        return 1
    fi
    
    print_status "Showing live logs from: $LOG_FILE"
    print_status "Press Ctrl+C to exit log view"
    echo "=================================="
    tail -f "$LOG_FILE"
}

# Main script logic
case "${1:-status}" in
    start)
        start_fetcher
        ;;
    stop)
        stop_fetcher
        ;;
    restart)
        restart_fetcher
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    help|--help|-h)
        echo "JSON Data Fetcher Service Manager"
        echo "================================="
        echo ""
        echo "This script manages a 24/7 JSON data fetcher that continuously"
        echo "retrieves live sports data and saves it to timestamped JSON files."
        echo ""
        echo "Usage: $0 {start|stop|restart|status|logs|help}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the 24/7 JSON data fetcher"
        echo "            • Creates virtual environment if needed"
        echo "            • Installs required dependencies"
        echo "            • Starts fetcher in background"
        echo ""
        echo "  stop    - Stop the data fetcher gracefully"
        echo "            • Sends SIGTERM for graceful shutdown"
        echo "            • Forces kill if necessary"
        echo ""
        echo "  restart - Restart the data fetcher"
        echo "            • Stops current process"
        echo "            • Starts new process"
        echo ""
        echo "  status  - Show current status and information"
        echo "            • Process status and details"
        echo "            • Recent log entries"
        echo "            • Generated files"
        echo "            • Virtual environment info"
        echo ""
        echo "  logs    - Show live log output"
        echo "            • Real-time log monitoring"
        echo "            • Press Ctrl+C to exit"
        echo ""
        echo "  help    - Show this help message"
        echo ""
        echo "Files:"
        echo "  Script:     $PYTHON_SCRIPT"
        echo "  Log file:   $LOG_FILE"
        echo "  PID file:   $PID_FILE"
        echo "  Virtual env: $VENV_DIR"
        echo ""
        echo "The data fetcher runs continuously, fetching data every 60 seconds"
        echo "and saving timestamped JSON files until manually stopped."
        echo ""
        echo "Environment variables can be configured in .env file."
        exit 0
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|help}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the 24/7 JSON data fetcher"
        echo "  stop    - Stop the data fetcher"
        echo "  restart - Restart the data fetcher"
        echo "  status  - Show current status and recent activity"
        echo "  logs    - Show live log output"
        echo "  help    - Show detailed help information"
        echo ""
        echo "The data fetcher runs continuously, fetching data every 60 seconds"
        echo "and saving timestamped JSON files until manually stopped."
        echo ""
        echo "Use './start.sh help' for more detailed information."
        exit 1
        ;;
esac

exit $?
