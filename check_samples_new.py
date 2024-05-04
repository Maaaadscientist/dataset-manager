import os,sys
import subprocess

# Define directories
mac_dir = '/Users/allen/opendata'
HD1_dir = '/Volumes/wd/CMSOpenData'
HD2_dir = '/Volumes/ExtDisk-1/opendata'

# Check which storage to use based on mount status
if len(sys.argv) > 1:
    root_dir = os.path.abspath(sys.argv[1])
elif os.path.exists(HD1_dir):
    root_dir = HD1_dir
elif os.path.exists(HD2_dir):
    root_dir = HD2_dir
else:
    root_dir = mac_dir

datasets_dir = os.path.join(root_dir, 'datasets')
download_dir = os.path.join(root_dir, 'samples')
processed_dir = os.path.join(root_dir, 'processed')

# Ensure the processed directory exists
os.makedirs(processed_dir, exist_ok=True)

def get_remote_file_size(url):
    path_part = url.split('eospublic.cern.ch/')[1]
    # Construct the full path for xrdfs
    full_url = f"root://eospublic.cern.ch/{path_part}"
    try:
        # Use subprocess to call xrdfs and parse the output
        result = subprocess.run(
            ['xrdfs', 'root://eospublic.cern.ch', 'stat', path_part],
            capture_output=True, text=True, check=True
        )
        # Parse the output to find the size
        for line in result.stdout.split('\n'):
            if 'Size:' in line:
                size = int(line.split()[1])
                return size
    except subprocess.CalledProcessError as e:
        print(f"Error accessing {full_url}: {e}")
        return None

# Function to process each dataset file and create a processed counterpart
def process_dataset_file(original_file_path, processed_file_path):
    with open(original_file_path, 'r') as file:
        lines = file.readlines()

    # Extract dataset and version info from the filename for directory structure in samples
    main_dir, sub_dir = os.path.basename(original_file_path).split('_file_index.txt')[0].rsplit('_', 1)

    dataset_base_path = os.path.join(download_dir, main_dir + "/" + sub_dir)

    # List of processed root file paths
    processed_root_paths = []

    size_mismatch_paths = []
    # Check each root file path in the dataset file
    for line in lines:
        root_file_path = line.strip()
        root_file_name = root_file_path.split('/')[-1]
        local_root_file_path = os.path.join(dataset_base_path, root_file_name)

        # Check if the root file has been downloaded and matches the expected size
        if os.path.exists(local_root_file_path):
            local_file_size = os.path.getsize(local_root_file_path)
            remote_file_size = get_remote_file_size(root_file_path)

            if local_file_size == remote_file_size:
                processed_root_paths.append(root_file_path + '\n')
            else:
                print("mismatch:", local_root_file_path, " local:", local_file_size, " remote:", remote_file_size)
                size_mismatch_paths.append(local_root_file_path)

        # Log or handle the size mismatches
    if size_mismatch_paths:
        print(f"Size mismatch found for the following files:")
        for mismatch in size_mismatch_paths:
            print(mismatch)
    # Write the processed dataset file with only the downloaded root file paths, if any
    if processed_root_paths:
        if os.path.isfile(processed_file_path):
            os.system(f"rm {processed_file_path}")
        with open(processed_file_path, 'w') as file:
            file.writelines(processed_root_paths)
        print(f"Created processed file for {os.path.basename(original_file_path)}")
    else:
        if os.path.isfile(processed_file_path):
            os.system(f"rm {processed_file_path}")
        print(f"No processed paths for {os.path.basename(original_file_path)}; no file created.")
# Process each dataset file
for dataset_file in os.listdir(datasets_dir):
    if dataset_file.endswith('_file_index.txt') and dataset_file.startswith('CMS'):
        original_file_path = os.path.join(datasets_dir, dataset_file)
        processed_file_path = os.path.join(processed_dir, dataset_file)
        process_dataset_file(original_file_path, processed_file_path)

print("Processed dataset files have been created in the 'processed' directory.")

