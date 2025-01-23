import os
import json
import argparse
import fcntl
from datetime import datetime


def load_events(file_path):
    """
    Reads and returns the list of events from the file.
    If the file is empty or does not exist, returns an empty list.
    """
    if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
        return []

    with open(file_path, "r") as f:
        try:
            events = json.load(f)
            return events if isinstance(events, list) else []
        except json.JSONDecodeError:
            return []  # Return empty list if the file contains invalid JSON


def save_events(events, file_path):
    """
    Saves the list of events back to the file in a formatted JSON array.
    """
    with open(file_path, "w") as f:
        # Lock the file for writing
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(events, f)
        fcntl.flock(f, fcntl.LOCK_UN)


def write_event(event, file_path="metadata.json"):
    """
    Reads the current file content, appends a new event, and writes it back.
    """
    # Load existing events from the file
    events = load_events(file_path)

    # Append the new event with a timestamp
    timestamp = datetime.utcnow().isoformat()
    event_with_timestamp = {"timestamp": timestamp, **event}
    events.append(event_with_timestamp)

    # Save the updated list of events back to the file
    save_events(events, file_path)
