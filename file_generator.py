import os
import random
import string

def generate_random_file(file_path, file_size_in_bytes):
    # Create a random file of the given size
    with open(file_path, 'w') as file:
        total_written = 0
        while total_written < file_size_in_bytes:
            # Generate a random string of 1024 characters (1 KB)
            random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=1024))
            # Write to the file
            file.write(random_string + '\n')
            total_written += len(random_string) + 1  # +1 for the newline character
        print(f"File '{file_path}' of size {file_size_in_bytes} bytes generated successfully.")

# Example usage:
file_size = 350 * 1024 * 1024  # 700 MB in bytes
file_path = "test.txt"  # Path to save the file

generate_random_file(file_path, file_size)
