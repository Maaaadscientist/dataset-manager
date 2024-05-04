import os, sys

# Define directories
mac_dir = '/Users/allen/opendata'
HD1_dir = '/Volumes/wd/CMSOpenData'
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
processed_dir = "outputs"#= os.path.join(root_dir, 'processed')

# Ensure the processed directory exists
os.makedirs(processed_dir, exist_ok=True)

# Helper function to convert bytes to gigabytes
def bytes_to_mb(bytes_size):
    return round(bytes_size / (1024 ** 2), 1)  # Convert to GB and round to 3 decimal places

# Function to process each dataset file and create a processed counterpart
def process_dataset_file(original_file_path, processed_file_path):
    with open(original_file_path, 'r') as file:
        lines = file.readlines()

    # Extract dataset and version info from the filename for directory structure in samples
    full_name = os.path.basename(original_file_path).split('_file_index.txt')[0]
    main_dir, sub_dir = full_name.rsplit('_', 1)
    dataset_base_path = os.path.join(download_dir, main_dir + "/" + sub_dir)

    # Prepare dictionary to hold processed root file paths by main_dir
    processed_files_dict = {}

    # Check each root file path in the dataset file
    for line in lines:
        root_file_path = line.strip()
        root_file_name = root_file_path.split('/')[-1]
        local_root_file_path = os.path.join(dataset_base_path, root_file_name)

        # Check if the root file has been downloaded
        if os.path.exists(local_root_file_path):
            file_size = bytes_to_mb(os.path.getsize(local_root_file_path))
            path_with_size = f"{os.path.abspath(local_root_file_path)},{file_size}MB\n"
            if main_dir not in processed_files_dict:
                processed_files_dict[main_dir] = []
            processed_files_dict[main_dir].append(path_with_size)

    # Write grouped dataset files
    for group_dir, paths in processed_files_dict.items():
        simplified_name = simplify_name(group_dir)
        group_file_path = os.path.join(processed_dir, f"{simplified_name}.txt")
        with open(group_file_path, 'w') as file:
            file.writelines(paths)
        print(f"Created grouped processed file for {simplified_name}")

# Simplify dataset file names based on the content
def simplify_name(name):
    if "Run201" in name:
        # Collision data
        parts = name.split('_')
        return 'Data_' + '_'.join(parts[1:3])  # e.g., Run2016H_SingleMuon
    else:
        # MC data
        parts = name.split('NanoAODv9_')[-1]
        parts = parts.split('_NANOAODSIM')[0]
        return 'SIM_' + parts  # Remove 'CMS', version, and processing tags

# Process each dataset file
for dataset_file in os.listdir(datasets_dir):
    if dataset_file.endswith('_file_index.txt') and dataset_file.startswith('CMS'):
        original_file_path = os.path.join(datasets_dir, dataset_file)
        processed_file_path = os.path.join(processed_dir, dataset_file.replace('CMS_', ''))
        process_dataset_file(original_file_path, processed_file_path)

print("Processed dataset files have been created in the 'processed' directory.")
