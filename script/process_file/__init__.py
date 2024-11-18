import os
import json
from typing import Callable

def read_json_and_process_data(src_file_path: str, process_json_data, should_skip_file: Callable[str, bool]=lambda _: False) -> str:
    if should_skip_file(src_file_path):
        return ""

    # Read JSON data from the file
    with open(src_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    # Prepare CSV data
    csv_data = process_json_data(src_file_path, json_data)

    # Convert CSV data to string format
    csv_output = ''# 'state,number of transactions,value of all transactions\n'
    for row in csv_data:
        csv_output += ','.join(map(str, row)) + '\n'

    return csv_output  # Return CSV content as a string


def copy_and_process_files(src_dir, dest_dir, dest_file_name, process_file: Callable[str, str], content_header):
    """
    Recursively scans through the src_dir, processes each JSON file, 
    and saves the output as CSV in the dest_dir while preserving the original directory structure.
    """
    print(src_dir, dest_dir)
    os.makedirs(dest_dir, exist_ok=True)

    processed_content = content_header

    for root, dirs, files in os.walk(src_dir):
        # # Determine the relative path from the source directory
        # relative_path = os.path.relpath(root, src_dir)
        # # Determine the corresponding destination directory path
        # dest_path = os.path.join(dest_dir, relative_path)
        
        # Create the destination directory if it doesn't exist

        for file in files:
            if file.endswith('.json'):  # Only process JSON files
                src_file_path = os.path.join(root, file)

                # Process the file to convert JSON to CSV content
                processed_content += process_file(src_file_path)
        
    dest_file_path = os.path.join(dest_dir, f"{dest_file_name}.csv")
    print(f"Will save file to: {dest_file_path}")
    print(dest_file_path)

    # Save the processed content to the destination directory
    with open(dest_file_path, 'w', encoding='utf-8') as f:
        f.write(processed_content)

    print(f"Processed file saved to: {dest_file_path}")

# # Example usage
# source_directory = f'{REPO_ROOT}/{DATA_PATH}'
# destination_directory = f'{OUTPUT_ROOT}/{DATA_PATH}'

# copy_and_process_files(source_directory, destination_directory)
