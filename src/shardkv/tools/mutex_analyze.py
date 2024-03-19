import sys

from datetime import datetime

acquired_locks = {}

def analyze_mutex(lock_log_file):
    # Open the log file for reading
    with open(lock_log_file, 'r') as file:
        # Iterate through each line in the file
        for line in file:
            # Split the line by spaces
            parts = line.split()

            if len(parts) < 9:
                continue
            # Extract relevant information.
            action = parts[5]
            lock = parts[8]

            # Check if a lock was acquired or released
            if action == "acquired":
                # Store the lock and its part in the dictionary
                acquired_locks[lock] = line
            elif action == "released":
                # Remove the lock from the dictionary if it was released
                acquired_locks.pop(lock, None)

    # Print any locks that were acquired but not released
    if acquired_locks:
        print("The following locks were acquired but not released:")
        for lock, context in acquired_locks.items():
            print(f"Lock: {lock}, [Context] {context}")
    else:
        print("All acquired locks were properly released.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} lock_log_file")
    else:
        lock_log_file = sys.argv[1]
        analyze_mutex(lock_log_file)
