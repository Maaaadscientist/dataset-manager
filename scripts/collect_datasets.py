import os

# Define directories
HD1_dir = '/media/exfat/CMSOpenData'

# Check which storage to use based on mount status
if os.path.exists(HD1_dir):
    root_dir = HD1_dir
else:
    print("Error: No suitable storage found.")
    exit(1)  # Exit the script if no storage is found

datasets_dir = os.path.join(root_dir, 'datasets')
download_dir = os.path.join(root_dir, 'samples')
processed_dir = os.path.join(root_dir, 'processed')
outputs_dir = 'path_outputs'

# Ensure the processed and outputs directories exist
os.makedirs(processed_dir, exist_ok=True)
if os.path.exists(outputs_dir):
    os.system(f'rm -rf {outputs_dir}')
os.makedirs(outputs_dir, exist_ok=True)

# Helper function to convert bytes to megabytes
def bytes_to_mb(bytes_size):
    return round(bytes_size / (1024 ** 2), 1)  # Convert to MB and round to 1 decimal place

# Function to process each dataset file and collect real paths if fully processed
def process_dataset_file(original_file_path, processed_file_path):
    with open(original_file_path, 'r') as file:
        original_lines = file.readlines()

    if os.path.exists(processed_file_path):
        with open(processed_file_path, 'r') as file:
            processed_lines = file.readlines()

        # Check if the processed file has the same number of lines as the original
        if len(processed_lines) == len(original_lines):
            collect_real_paths(original_file_path, original_lines)
        else:
            full_name = os.path.basename(original_file_path).split('_file_index.txt')[0]
            main_dir, sub_dir = full_name.rsplit('_', 1)
            simplified_name = simplify_name(main_dir)
            print(simplified_name+"/"+sub_dir, "file number doesn't match", len(processed_lines), "/", len(original_lines))

def collect_real_paths(original_file_path, lines):
    # Extract dataset and version info from the filename for directory structure in samples
    full_name = os.path.basename(original_file_path).split('_file_index.txt')[0]
    main_dir, sub_dir = full_name.rsplit('_', 1)
    dataset_base_path = os.path.join(download_dir, main_dir, sub_dir)

    processed_files_list = []

    # Collect real paths of downloaded files
    for line in lines:
        root_file_path = line.strip()
        root_file_name = root_file_path.split('/')[-1]
        local_root_file_path = os.path.join(dataset_base_path, root_file_name)

        # Ensure the file exists before adding to list
        if os.path.exists(local_root_file_path):
            file_size = bytes_to_mb(os.path.getsize(local_root_file_path))
            path_with_size = f"{os.path.abspath(local_root_file_path)},{file_size}MB"
            processed_files_list.append(path_with_size)

    # Write collected paths to the output directory
    output_file_path = os.path.join(outputs_dir, f"{simplify_name(main_dir)}.txt")
    with open(output_file_path, 'a') as file:
        for path in processed_files_list:
            file.write(path + "\n")
    print(f"{simplify_name(main_dir)}/{sub_dir} completed.")

# Simplify dataset file names based on the content
def simplify_name(name):
    if "Run201" in name:
        # Collision data
        parts = name.split('_')
        return 'Data_' + '_'.join(parts[1:3])  # e.g., Run2016H_SingleMuon
    elif 'CMS_mc' in name:
        # MC data
        parts = name.split('NanoAODv9_')[-1]
        parts = parts.split('_NANOAODSIM')[0]
        return 'SIM_' + parts  # Remove 'CMS', version, and processing tags

# Process each dataset file
for dataset_file in os.listdir(datasets_dir):
    if dataset_file.endswith('_file_index.txt') and dataset_file.startswith('CMS'):
        original_file_path = os.path.join(datasets_dir, dataset_file)
        processed_file_path = os.path.join(processed_dir, dataset_file)
        process_dataset_file(original_file_path, processed_file_path)


