import os
import logging
from tqdm import tqdm
import fnmatch
import numpy as np
import rasterio
from rasterio.mask import mask
import fnmatch
import torch
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms
import torch
import random
import torch.optim as optim
import torch.nn.functional as F
import torch.optim as optim
from torchvision.transforms import GaussianBlur, ColorJitter, RandomHorizontalFlip, RandomVerticalFlip, RandomRotation
import torch
from torch.utils.data import DataLoader
import torch.nn.functional as F
import matplotlib.pyplot as plt

import tempfile
import xarray as xr
import rioxarray

from noachdm.messaging.kafka_producer import KafkaProducer
from noachdm.messaging.message import Message

from kafka.errors import NoBrokersAvailable

logger = logging.getLogger(__name__)


def percentile_stretch(band_array, lower_percentile=0, upper_percentile=99):
    """Applies a percentile stretch to the band array for better contrast."""
    lower_bound = np.percentile(band_array, lower_percentile)
    upper_bound = np.percentile(band_array, upper_percentile)
    
    # Clip values to the calculated percentiles
    band_array = np.clip(band_array, lower_bound, upper_bound)
    
    # Normalize to the range 0-1
    return (band_array - lower_bound) / (upper_bound - lower_bound)

# Helper function for gamma correction
def gamma_correction(rgb_image, gamma=0.8):
    """Apply gamma correction to enhance brightness and contrast."""
    return np.power(rgb_image, 1 / gamma)

# Helper function for saturation adjustment
def adjust_saturation(rgb_image, factor=1.2):
    """Adjust the saturation of the RGB image."""
    grayscale = np.mean(rgb_image, axis=0)  # Get the luminance (mean of R, G, B)
    return np.clip((rgb_image - grayscale) * factor + grayscale, 0, 1)  # Adjust and clip

def scale_rgb_image(rgb_image):

    # Applies a percentile stretch to the band array for better contrast
    # rgb_image = np.array([percentile_stretch(rgb_image[i], upper_percentile=98) for i in range(3)])

     # Normalize the image to the range 0-1 for display
    rgb_image = (rgb_image - np.min(rgb_image)) / (np.max(rgb_image) - np.min(rgb_image))
    
    # # Apply gamma correction
    rgb_image = gamma_correction(rgb_image, gamma=1.5)
    
    # # Adjust saturation for more vivid colors
    rgb_image = adjust_saturation(rgb_image, factor=1.1)
    
    # Convert the image to the range 0-255
    rgb_image = (rgb_image * 255).astype(np.uint8)

    return rgb_image

# Initialize model, loss, and optimizer
# model = SiamUnet_conc(input_nbr=3, label_nbr=2)  # Assuming 3 input channels (RGB) and 2 labels (change/no change)
# criterion = F.cross_entropy() # Adjust based on your output label format

def cross_entropy(input, target, weight=None, reduction='mean',ignore_index=255):
    """
    logSoftmax_with_loss
    :param input: torch.Tensor, N*C*H*W
    :param target: torch.Tensor, N*1*H*W,/ N*H*W
    :param weight: torch.Tensor, C
    :return: torch.Tensor [0]
    """
    target = target.long()
    if target.dim() == 4:
        target = torch.squeeze(target, dim=1)
    if input.shape[-1] != target.shape[-1]:
        input = F.interpolate(input, size=target.shape[1:], mode='bilinear',align_corners=True)

    return F.cross_entropy(input=input, target=target, weight=weight,
                           ignore_index=ignore_index, reduction=reduction)

def train_model(model, train_dataset, val_dataset, num_epochs=10, batch_size=4, device='cpu', lr=0.001):
    """
    Train the model with both training and validation datasets.

    Args:
        model: The neural network model to train.
        train_dataset: The dataset for training.
        val_dataset: The dataset for validation.
        optimizer: The optimizer used for updating the model's parameters.
        num_epochs: Number of epochs to train the model.
        batch_size: Batch size for loading data.
        device: The device to run the model on ('cuda' or 'cpu').
    """
    # Create DataLoader for training and validation datasets
    train_dataloader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

    model.to(device)  # Move model to the specified device (GPU or CPU)
    model.train()  # Set the model to training mode
    optimizer = optim.Adam(model.parameters(), lr=lr)
    

    for epoch in range(num_epochs):
        running_loss = 0.0
        model.train()  # Set model to training mode for each epoch
        
        # Training phase
        for pre_image, post_image, target, _, _ in train_dataloader:
            pre_image, post_image, target = pre_image.to(device), post_image.to(device), target.to(device)
            optimizer.zero_grad()  # Clear gradients from the previous step
    
            # Forward pass through the model
            output = model(pre_image, post_image)
    
            # Ensure the target is a float tensor
            target = target.squeeze(1).float()  # Convert target to float tensor if it's not

            # Define class weights, e.g., [weight_for_0, weight_for_1]
            # class_weights = torch.tensor([1.0, 5.0], device=device)  
            
            # Compute the loss between output and target
            # loss = F.cross_entropy(output, target.long(), weight=class_weights)  # Compute loss
            loss = F.cross_entropy(output, target.long())  # Compute loss
    
            # Backward pass and optimization
            loss.backward()  # Backpropagate the loss
            optimizer.step()  # Update the model's parameters
            
            running_loss += loss.item()

        if val_dataset:
            val_dataloader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
        
            # Validation phase
            model.eval()  # Set model to evaluation mode
            val_loss = 0.0
            with torch.no_grad():  # Disable gradient calculation for validation
                for pre_image, post_image, target, _, _ in val_dataloader:
                    pre_image, post_image, target = pre_image.to(device), post_image.to(device), target.to(device)
        
                    # Forward pass through the model
                    output = model(pre_image, post_image)
        
                    # Ensure the target is a float tensor
                    target = target.squeeze(1).float()
    
                    # Compute the loss between output and target for validation
                    loss = F.cross_entropy(output, target.long())  # Compute loss
                    val_loss += loss.item()
    
           
            print(f'Epoch [{epoch+1}/{num_epochs}], Training Loss: {running_loss/len(train_dataloader):.4f}, Validation Loss: {val_loss/len(val_dataloader):.4f}')

        else:
            print(f'Epoch [{epoch+1}/{num_epochs}], Training Loss: {running_loss/len(train_dataloader):.4f}')


class CustomTransform:
    def __init__(self, flip_horizontal=True, flip_vertical=True, angles=(0, 90), 
                 gaussian_blur=True, color_jitter=True):
        self.flip_horizontal = flip_horizontal
        self.flip_vertical = flip_vertical
        self.horizontal_flip = RandomHorizontalFlip(p=0.5) if flip_horizontal else None
        self.vertical_flip = RandomVerticalFlip(p=0.5) if flip_vertical else None
        self.angles = angles if angles else None
        self.rotation = RandomRotation(degrees=self.angles) if self.angles else None
        self.gaussian_blur = gaussian_blur
        self.color_jitter = color_jitter
        
        # Initialize Gaussian Blur and Color Jitter transformations
        self.blur_transform = GaussianBlur(kernel_size=(5, 5), sigma=(0.1, 2.0)) if gaussian_blur else None
        self.color_jitter_transform = ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2, hue=0.1) if color_jitter else None

    def __call__(self, sample):
        pre_image = sample['pre_image']
        post_image = sample['post_image']
        label = sample['label']

        stacked_images = torch.stack([pre_image, post_image, label.repeat(3, 1, 1)], dim=0)

        # Apply horizontal flip
        if self.flip_horizontal:
            stacked_images = self.horizontal_flip(stacked_images)
            
        # Apply vertical flip
        if self.flip_vertical:
            stacked_images = self.vertical_flip(stacked_images)

        # Apply random rotation from the specified angles
        if self.angles and random.random() < 0.5:
            stacked_images = self.rotation(stacked_images)

        pre_image, post_image, label = stacked_images[0], stacked_images[1], stacked_images[2]
        label = label[0].squeeze(0)
        
        stacked_images = torch.stack([pre_image, post_image], dim=0)
        
        # Apply Gaussian blur
        if self.gaussian_blur and random.random() < 0.5:
            stacked_images = self.blur_transform(stacked_images)

        # Apply random color jitter
        if self.color_jitter and random.random() < 0.5:
            stacked_images = self.color_jitter_transform(stacked_images)
            
        pre_image, post_image = stacked_images[0], stacked_images[1]

        return {'pre_image': pre_image, 'post_image': post_image, 'label': label}


# Example usage

class ChangeDetectionDataset(Dataset):
    def __init__(self, data_info, ndvi_threshold, ndvi_diff_threshold, bsi_threshold, bsi_diff_threshold, transform=None):
        """
        Args:
            data_info (list of tuples): List of tuples (image_files, field_mask_file).
            ndvi_threshold, ndmi_threshold, bsi_threshold, bsi_diff_threshold: Parameters for processing images.
            transform (callable, optional): Transformations to apply to pre/post images.
        """
        self.data_info = data_info
        self.ndvi_threshold = ndvi_threshold
        self.ndvi_diff_threshold = ndvi_diff_threshold
        self.bsi_threshold = bsi_threshold
        self.bsi_diff_threshold = bsi_diff_threshold
        self.transform = transform  # Data augmentation transformations (optional)

    def __len__(self):
        return len(self.data_info)

    def __getitem__(self, idx):
        # Get file paths and mask file for the current index
        image_files, field_mask_file, crop_mask_file, image_dates = self.data_info[idx]

        out_before, out_after, ndvi_before, ndvi_after, bsi_before, bsi_after, mask_before, mask_after, field_mask, crop_mask, result = process_all_images(
            image_files, field_mask_file, crop_mask_file,
            self.ndvi_threshold, self.ndvi_diff_threshold,
            self.bsi_threshold, self.bsi_diff_threshold
        )

        # Convert inputs to tensors
        pre_image = torch.tensor(out_before, dtype=torch.float32) / 15000
        post_image = torch.tensor(out_after, dtype=torch.float32) / 15000
        label = torch.tensor(result, dtype=torch.long)

        # Apply transformations (if any)
        if self.transform:
            # Combine images and label into a dictionary
            sample = {'pre_image': pre_image, 'post_image': post_image, 'label': label}
    
            # Apply transformations
            sample = self.transform(sample)
    
            # Unpack transformed images and label
            pre_image, post_image, label = sample['pre_image'], sample['post_image'], sample['label']

        return pre_image, post_image, label, field_mask_file, crop_mask_file, image_dates


def calculate_ndvi(b4, b8):
    ndvi = (b8 - b4) / (b8 + b4 + 1e-10)  # Adding a small constant to avoid division by zero
    return ndvi


def calculate_bsi(b2, b4, b8, b12):
    bsi = ((b12 + b4) - (b8 + b2)) / ((b12 + b4) + (b8 + b2) + 1e-10)
    return bsi

def check_cloud_cover(scl, threshold=10):
    valid_pixels = np.isin(scl, [4, 5, 6])  # Only consider non-cloud values
    cloud_coverage = 100 * (1 - np.sum(valid_pixels) / valid_pixels.size)  # Percentage of cloud pixels
    # print(cloud_coverage, cloud_coverage < threshold)
    # plt.figure(figsize=(4,4))
    # plt.imshow(valid_pixels, cmap='gray')
    # plt.show()
    return cloud_coverage < threshold

def check_cloud_cases(image_files, threshold=10):
    cloud_check = []
    for image_file in image_files:
        with rasterio.open(image_file) as src:
            cloud_check.append(check_cloud_cover(src.read(6), threshold))
    return cloud_check

def process_pair(bands_t1, bands_t2, field_mask, ndvi_thresh, ndvi_diff_thresh, bsi_thresh, bsi_diff_thresh):
    b2_t1, b3_t1, b4_t1, b8_t1, b12_t1, scl_t1 = bands_t1
    b2_t2, b3_t2, b4_t2, b8_t2, b12_t2, scl_t2 = bands_t2

    ndvi_t1 = calculate_ndvi(b4_t1, b8_t1)
    ndvi_t2 = calculate_ndvi(b4_t2, b8_t2)


    bsi_t1 = calculate_bsi(b2_t1, b4_t1, b8_t1, b12_t1)
    bsi_t2 = calculate_bsi(b2_t2, b4_t2, b8_t2, b12_t2)


    # Calculate NDVI and BSI differences
    ndvi_diff = ndvi_t2 - ndvi_t1
    bsi_diff = bsi_t2 - bsi_t1

    cloud_mask_t1 = np.isin(scl_t1, [4, 5, 6])
    cloud_mask_t2 = np.isin(scl_t2, [4, 5, 6])

    # Initialize result array for marking pixels
    marked_pixels = np.zeros_like(field_mask)

    # Iterate over each field (unique ID in field_mask)
    unique_fields = sorted(np.unique(field_mask))
    for f_i,field_id in enumerate(unique_fields):
        if field_id == 0:  # Skip background
            continue
        
        field_mask_bool = (field_mask == field_id)
        
        mean_ndvi_pre = np.mean(ndvi_t1[field_mask_bool])
        mean_ndvi_post = np.mean(ndvi_t2[field_mask_bool])
        mean_ndvi_diff = np.mean(ndvi_diff[field_mask_bool])

        mean_bsi_pre = np.mean(bsi_t1[field_mask_bool])
        mean_bsi_post = np.mean(bsi_t2[field_mask_bool])
        mean_bsi_diff = np.mean(bsi_diff[field_mask_bool])


        all_cloudfree_t1 = np.all(cloud_mask_t1[field_mask_bool]==1) 
        all_cloudfree_t2 = np.all(cloud_mask_t2[field_mask_bool]==1) 

        c_1 = mean_ndvi_diff <= ndvi_diff_thresh
        c_2 = mean_ndvi_pre >= ndvi_thresh
        # c_2 = True
        c_3 = mean_ndvi_post < ndvi_thresh
        # c_3 = True
        c_4 = mean_bsi_diff >= bsi_diff_thresh
        c_5 = mean_bsi_pre <= bsi_thresh
        c_6 = mean_bsi_post > bsi_thresh
        c_7 = all_cloudfree_t1
        c_8 = all_cloudfree_t2

        c_all = np.array([c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8])

        if np.sum(c_all) == len(c_all):
            marked_pixels[field_mask_bool] = 1

    return marked_pixels

def process_all_images(image_files, field_mask_file, crop_id_file, ndvi_thresh, ndvi_diff_thresh, bsi_thresh, bsi_diff_thresh):
    # Load field mask (single file)
    with rasterio.open(field_mask_file) as mask_src:
        field_mask = mask_src.read(1)

    with rasterio.open(crop_id_file) as crop_src:
        crop_mask = crop_src.read(1)

    bands_t1 = []
    bands_t2 = []
    
    # Assuming the 6 bands are ordered B02, B03, B04, B08, B12, SCL
    with rasterio.open(image_files[0]) as src:
        for band in range(6):  
            bands_t1.append(src.read(band + 1))
            
    with rasterio.open(image_files[1]) as src:
        for band in range(6): 
            bands_t2.append(src.read(band + 1))

    # Process the pair
    result = process_pair(bands_t1, bands_t2, field_mask, ndvi_thresh, ndvi_diff_thresh, bsi_thresh, bsi_diff_thresh)

    out_before = np.array(bands_t1)[[2,1,0]]
    out_after = np.array(bands_t2)[[2,1,0]]

    # out_before = np.array(bands_t1)[:-1]
    # out_after = np.array(bands_t2)[:-1]

    mask_before = np.array(bands_t1)[-1]
    mask_before = np.isin(mask_before, [4, 5, 6]) 
    
    mask_after = np.array(bands_t2)[-1]
    mask_after = np.isin(mask_after, [4, 5, 6])

    b2_t1, b3_t1, b4_t1, b8_t1, b12_t1, scl_t1 = bands_t1
    b2_t2, b3_t2, b4_t2, b8_t2, b12_t2, scl_t2 = bands_t2

    ndvi_before = calculate_ndvi(b4_t1, b8_t1)
    ndvi_after = calculate_ndvi(b4_t2, b8_t2)

    bsi_before = calculate_bsi(b2_t1, b4_t1, b8_t1, b12_t1)
    bsi_after = calculate_bsi(b2_t2, b4_t2, b8_t2, b12_t2)

    # return out_before, out_after, result 
    return out_before, out_after, ndvi_before, ndvi_after, bsi_before, bsi_after, mask_before, mask_after, field_mask, crop_mask, result


def send_kafka_message(bootstrap_servers, topic, product_path):
    schema_def = Message.schema_response()

    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, schema=schema_def)
        kafka_message = {"chdm_product_path": product_path}
        producer.send(topic=topic, key=None, value=kafka_message)
    except NoBrokersAvailable as e:
        logger.warning("No brokers available. Continuing without Kafka. Error: %s", e)
        producer = None


def crop_and_make_mosaic(items_paths, bbox) -> tempfile.TemporaryDirectory:
    """
    There is a lower (hardcoded for now) limit on kernel for images.
    Even though we say crop, if the bbox
    is smaller than this lower limit, we apply the lower limit instead.
    Moreover, this function crops and then combines the images in a mosaic,
    by applying a median calculation.
    This is true in either adjacent tiles case, or in multidate extent cases,
    where the exact requested date was not found
    """

    temp_dir = tempfile.TemporaryDirectory()
    bands = ("B02", "B03", "B04")
    for band in bands:
        cropped_list = []
        for path in items_paths:
            # TODO construct band file path
            da = rioxarray.open_rasterio(path, masked=True).squeeze()
            # da = da.rio.clip_box(minx=bbox[0], miny=bbox[1], maxx=bbox[2], maxy=bbox[3])
            da = da.rio.clip_box(*bbox)
            cropped_list.append(da)

        # If more than one path (bbox exceeds one tile or multiple dates)
        if len(cropped_list) > 1:
            stacked = xr.concat(cropped_list, dim='stack')
            result = stacked.median(dim='stack')
        else:
            result = cropped_list[0]

        # Ensure result has spatial reference info
        result.rio.write_crs(result.rio.crs, inplace=True)

        # TODO add meaningful file name
        output_path = os.path.join(temp_dir.name, "cropped_.", band, ".tif")
        result.rio.to_raster(output_path)

    return temp_dir.name
