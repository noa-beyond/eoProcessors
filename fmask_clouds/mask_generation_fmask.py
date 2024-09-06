import os
import yaml
from fmask.cmdline import sentinel2Stacked
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Load configuration from YAML file
def load_config(config_file='config.yaml'):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def generate_cloud_mask(input_dir, output_dir):
    for safe in os.listdir(input_dir):
        if safe.endswith('.zip'):
            logging.info(f"Skipping ZIP file: {safe}")
            continue

        full_path = os.path.join(input_dir, safe)
        if not os.path.isdir(full_path):
            logging.warning(f"Skipping non-directory file: {safe}")
            continue

        # Extract date and tile info from SAFE file name
        out_img_file = f"{safe.split('_')[2][:8]}_{safe.split('_')[5][1:]}_cloudMask.img"
        out_img_path = os.path.join(output_dir, out_img_file)

        logging.info(f"Starting cloud mask generation for {safe} -> {out_img_file}...")

        # Call the Fmask function
        try:
            sentinel2Stacked.mainRoutine(['--safedir', full_path, '-o', out_img_path])
            logging.info(f"Completed running Fmask on {safe}")
        except Exception as e:
            logging.error(f"Error processing {safe}: {e}")

if __name__ == '__main__':
    # Load configuration
    config = load_config()

    # Ensure input and output directories exist
    input_dir = config['input_dir']
    output_dir = config['output_dir']

    # create output dir if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Start the cloud mask generation process
    generate_cloud_mask(input_dir, output_dir)
