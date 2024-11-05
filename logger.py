import sys

class Logger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.log = open(log_file, "a")

    def write(self, message):
        # Write to the log file only
        self.log.write(message)
        self.log.flush()  # Ensure it writes to the file in real time

    def flush(self):
        """Ensure compatibility with Python's flush behavior."""
        self.log.flush()
    
    def close(self):
        """Close the log file properly."""
        self.log.close()
