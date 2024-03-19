#!/bin/bash

# Check if the number of arguments is correct
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <timeout_seconds> <command>"
    exit 1
fi

# Assign command-line arguments to variables
timeout_seconds=$1
command_to_run=${@:2}

# Display command and timeout information to the user
echo "Executing the following command:"
echo "$command_to_run"
echo "Timeout: $timeout_seconds seconds"

# Run the command with timeout
if timeout "$timeout_seconds"s $command_to_run; then
    echo "Command completed successfully."
else
    echo "Command timed out."
    # Add additional actions here, such as sending a notification
fi
