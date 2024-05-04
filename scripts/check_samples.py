import os,sys

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
processed_dir = os.path.join(root_dir, 'processed')


# Ensure the processed directory exists
os.makedirs(processed_dir, exist_ok=True)

# Function to process each dataset file and create a processed counterpart
def process_dataset_file(original_file_path, processed_file_path):
    with open(original_file_path, 'r') as file:
        lines = file.readlines()

    # Extract dataset and version info from the filename for directory structure in samples
    main_dir, sub_dir = os.path.basename(original_file_path).split('_file_index.txt')[0].rsplit('_', 1)

    dataset_base_path = os.path.join(download_dir, main_dir + "/" + sub_dir)

    # List of processed root file paths
    processed_root_paths = []

    # Check each root file path in the dataset file
    for line in lines:
        root_file_path = line.strip()
        root_file_name = root_file_path.split('/')[-1]
        local_root_file_path = os.path.join(dataset_base_path, root_file_name)

        # Check if the root file has been downloaded
        if os.path.exists(local_root_file_path):
            processed_root_paths.append(root_file_path + '\n')

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

    ## Write the processed dataset file with only the downloaded root file paths
    #with open(processed_file_path, 'w') as file:
    #    file.writelines(processed_root_paths)

# Process each dataset file
for dataset_file in os.listdir(datasets_dir):
    if dataset_file.endswith('_file_index.txt') and dataset_file.startswith('CMS'):
        original_file_path = os.path.join(datasets_dir, dataset_file)
        print(original_file_path)
        processed_file_path = os.path.join(processed_dir, dataset_file)
        process_dataset_file(original_file_path, processed_file_path)
        print(f"Created processed file for {dataset_file}")

print("Processed dataset files have been created in the 'processed' directory.")

