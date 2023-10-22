#!/usr/bin/env bash

# Get the directory where this shell script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Assuming your virtual environment is named "venv" and located in the same directory
VENV_DIR="$DIR/.venv"

# Check if the virtual environment directory exists
if [ -d "$VENV_DIR" ]; then
    # Activate the virtual environment
    source "$VENV_DIR/bin/activate"
else
    echo "Virtual environment not found in $VENV_DIR"
    exit 1
fi

export TZ=Asia/Kolkata

python "$DIR/Runner.py" > "$DIR/Runner.log" 2>&1
