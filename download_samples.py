import os
import subprocess

# Define directories
mac_dir = '/Users/allen/opendata'
#HD1_dir = '/Volumes/wd/CMSOpenData'
HD1_dir = '/media/exfat/CMSOpenData'
HD2_dir = '/Volumes/ExtDisk-1/opendata'

# Check which storage to use based on mount status
if os.path.exists(HD1_dir):
    root_dir = HD1_dir
elif os.path.exists(HD2_dir):
    root_dir = HD2_dir
else:
    root_dir = mac_dir

datasets_dir = os.path.join(root_dir, 'datasets')
download_dir = os.path.join(root_dir, 'samples')
processed_dir = os.path.join(root_dir, 'processed')

# Ensure directories exist
os.makedirs(datasets_dir, exist_ok=True)
os.makedirs(download_dir, exist_ok=True)
os.makedirs(processed_dir, exist_ok=True)

# Function to check if a file is processed
def is_processed(dataset_file, file_path):
    processed_file_path = os.path.join(processed_dir, dataset_file)
    if os.path.exists(processed_file_path):
        with open(processed_file_path, 'r') as f:
            processed_files = {line.strip() for line in f}
        return file_path in processed_files
    return False

# Function to update the processed files list
def update_processed_list(dataset_file, file_path):
    processed_file_path = os.path.join(processed_dir, dataset_file)
    with open(processed_file_path, 'a') as f:
        f.write(f"{file_path}\n")

# Prompt the user to input a keyword
keyword = input("Enter a keyword to filter datasets: ")

# List all files in the datasets directory
dataset_files = [f for f in os.listdir(datasets_dir) if f.endswith('_file_index.txt')]

# Process each dataset file
for dataset_file in dataset_files:
    if keyword.lower() in dataset_file.lower():
        full_path = os.path.join(datasets_dir, dataset_file)
        
        # Parse the dataset name and version info from the filename
        main_dir, sub_dir = dataset_file.split('_file_index.txt')[0].rsplit('_', 1)
        main_dir_path = os.path.join(download_dir, main_dir)
        sub_dir_path = os.path.join(main_dir_path, sub_dir)
        os.makedirs(sub_dir_path, exist_ok=True)
        
        # Read and process each line in the dataset file
        with open(full_path, 'r') as file:
            for line in file:
                root_file_path = line.strip()
                root_file_name = root_file_path.split('/')[-1]
                local_root_file_path = os.path.join(sub_dir_path, root_file_name)
                
                # Check if the file has been marked as processed
                if is_processed(dataset_file, root_file_path):
                    print(f"Skipping download for {local_root_file_path}, already processed.")
                    continue
    
                # Download the root file using xrdcp if it does not exist
                if not os.path.exists(local_root_file_path):
                    command = ['xrdcp', root_file_path, local_root_file_path]
                    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as proc:
                        print(f"Starting download: {local_root_file_path}")
                        for output_line in proc.stdout:
                            print(output_line.strip())
                    print(f"Downloaded {root_file_name} to {local_root_file_path}")
                    update_processed_list(dataset_file, root_file_path)
                else:
                    print(f"{root_file_name} already exists in {local_root_file_path}")

print("All files processed and downloaded.")

