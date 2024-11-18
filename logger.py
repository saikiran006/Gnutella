class Logger:
    def __init__(self, log_file):
        self.log_file = log_file
        try:
            self.log = open(log_file, "a")  # Open the log file in append mode
        except IOError as e:
            print(f"Error opening log file {log_file}: {e}")
            sys.exit(1)  # Exit if the file cannot be opened

    def write(self, message):
        try:
            # Write to the log file only
            self.log.write(message)
            self.log.flush()  # Ensure it writes to the file in real time
        except IOError as e:
            print(f"Error writing to log file {self.log_file}: {e}")

    def flush(self):
        """Ensure compatibility with Python's flush behavior."""
        try:
            self.log.flush()
        except IOError as e:
            print(f"Error flushing log file {self.log_file}: {e}")

    def close(self):
        """Close the log file properly."""
        try:
            self.log.close()
        except IOError as e:
            print(f"Error closing log file {self.log_file}: {e}")
    
    def __del__(self):
        """Ensure file is closed when the object is deleted."""
        self.close()

    # Optional: Allow the Logger class to be used with `with` for context management
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
