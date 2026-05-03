#!/bin/bash

# Configuration
BASE_URL="http://localhost:8085/api/bitcask"

# Helper function to print usage
usage() {
    echo "Usage:"
    echo "  $0 --view-all"
    echo "  $0 --view --key=SOME_KEY"
    echo "  $0 --perf --clients=N"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

TIMESTAMP=$(date +%s)

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --view-all)
            ACTION="VIEW_ALL"
            shift
            ;;
        --view)
            ACTION="VIEW"
            shift
            ;;
        --key=*)
            KEY="${1#*=}"
            shift
            ;;
        --perf)
            ACTION="PERF"
            shift
            ;;
        --clients=*)
            CLIENTS="${1#*=}"
            shift
            ;;
        *)
            echo "Unknown parameter passed: $1"
            usage
            ;;
    esac
done

# Execute based on action
if [ "$ACTION" == "VIEW_ALL" ]; then
    FILENAME="${TIMESTAMP}.csv"
    echo "Fetching all records to ${FILENAME}..."
    curl -s -X GET "${BASE_URL}/all/csv" -o "$FILENAME"
    echo "Done."

elif [ "$ACTION" == "VIEW" ]; then
    if [ -z "$KEY" ]; then
        echo "Error: --key=SOME_KEY is required for --view"
        exit 1
    fi
    curl -s -X GET "${BASE_URL}/${KEY}"
    echo "" # Add a newline for terminal readability

elif [ "$ACTION" == "PERF" ]; then
    if [ -z "$CLIENTS" ]; then
         echo "Error: --clients=N is required for --perf"
         exit 1
    fi
    echo "Starting $CLIENTS concurrent clients..."
    
    # Launch clients in the background
    for (( i=1; i<=CLIENTS; i++ ))
    do
        FILENAME="${TIMESTAMP}_thread_${i}.csv"
        curl -s -X GET "${BASE_URL}/all/csv" -o "$FILENAME" &
    done
    
    # Wait for all background threads to finish
    wait
    echo "All $CLIENTS threads completed."

else
    usage
fi