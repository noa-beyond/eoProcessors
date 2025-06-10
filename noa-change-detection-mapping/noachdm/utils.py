import os
import logging
import tempfile
import random
import string
import pathlib
from collections import defaultdict

import numpy as np
import rasterio
import xarray as xr
import rioxarray

import torch
from torch.utils.data import Dataset

from kafka.errors import NoBrokersAvailable

from noachdm.models.BIT import define_G

from noachdm.messaging.kafka_producer import KafkaProducer
from noachdm.messaging.message import Message

logger = logging.getLogger(__name__)


def send_kafka_message(bootstrap_servers, topic, order_id, product_path):
    schema_def = Message.schema_response()

    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, schema=schema_def)
        kafka_message = {
            "result": 0,
            "orderId": order_id,
            "chdm_product_path": product_path
            }
        producer.send(topic=topic, key=None, value=kafka_message)
    except NoBrokersAvailable as e:
        logger.warning("No brokers available. Continuing without Kafka. Error: %s", e)
        producer = None


def get_bbox(geometry):
    """
    Extract a bbox from a Geojson Geometry
    """
    def extract_coords(geom):
        coords = geom.get("coordinates", [])
        geometry_type = geom["type"]
        if geometry_type == "Point":
            return [coords]
        elif geometry_type in ["LineString", "MultiPoint"]:
            return coords
        elif geometry_type in ["Polygon", "MultiLineString"]:
            return [pt for ring in coords for pt in ring]
        elif geometry_type == "MultiPolygon":
            return [pt for poly in coords for ring in poly for pt in ring]
        elif geometry_type == "GeometryCollection":
            return [pt for g in geom["geometries"] for pt in extract_coords(g)]
        else:
            raise ValueError(f"Unsupported geometry type: {geometry_type}")

    all_coords = extract_coords(geometry)
    lons = [pt[0] for pt in all_coords]
    lats = [pt[1] for pt in all_coords]

    return tuple([min(lons), min(lats), max(lons), max(lats)])


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
    a_filename = ""
    for band in bands:
        cropped_list = []
        for path in items_paths:
            for raster in os.listdir(path):
                # TODO construct band file path
                if band in raster:
                    if a_filename == "":
                        a_filename = pathlib.Path(raster).name
                    da = rioxarray.open_rasterio(raster, masked=True).squeeze()
                    da = da.rio.clip_box(*bbox)
                    cropped_list.append(da)
                    continue

        # If more than one path (bbox exceeds one tile or multiple dates)
        if len(cropped_list) > 1:
            stacked = xr.concat(cropped_list, dim='stack')
            result = stacked.median(dim='stack')
        else:
            result = cropped_list[0]

        # Ensure result has spatial reference info
        result.rio.write_crs(result.rio.crs, inplace=True)

        # TODO add meaningful file name
        if len(cropped_list) > 1:
            a_date = a_filename.split("_")[-4]
            a_tile = a_filename.split("_")[-5]
            filename = "_".join(
                "composite",
                a_tile,
                a_date,
                "composite",
                "composite",
                band,
            )
        else:
            filename = a_filename.split(".")[0]
        output_path = os.path.join(temp_dir.name, filename + ".tif")
        result.rio.to_raster(output_path)

    return temp_dir.name


def closest_power_of_two(n):
    if n < 1:
        return 1  # Edge case: return 1 for 0 or negative input
    lower = 2 ** (n.bit_length() - 1)
    upper = 2 ** n.bit_length()
    return lower if n - lower < upper - n else upper


def closest_even(n):
    if n % 2 == 0:
        return n
    return n - 1 if n % 2 == 1 else n + 1


class SentinelChangeDataset(Dataset):
    def __init__(self, pre_dir, post_dir):

        self.pre_scenes = self._group_bands(pre_dir)
        self.post_scenes = self._group_bands(post_dir)

        # Dynamically compute patch sizes and strides per scene
        self.patch_coords = []

        for idx, scene in enumerate(self.pre_scenes):
            with rasterio.open(scene['B04']) as src:
                h, w = src.height, src.width
                min_dim = min(h, w)
                patch_size = max(128, min((3 * min_dim) // 4, 2048))
                # self.patch_size = closest_power_of_two(patch_size)
                self.patch_size = closest_even(patch_size)
                # self.patch_size = patch_size
                self.stride = (3 * self.patch_size) // 4
                self.stride = closest_even(self.stride)

                for y in range(0, h, self.stride):
                    for x in range(0, w, self.stride):
                        if y + self.patch_size > h:
                            y = h - self.patch_size
                        if x + self.patch_size > w:
                            x = w - self.patch_size
                        self.patch_coords.append((idx, y, x))

    def _group_bands(self, folder):

        grouped = defaultdict(dict)
        for fname in sorted(os.listdir(folder)):
            if not fname.endswith(".tif"):
                continue
            for band in ['B04', 'B03', 'B02']:
                if band in fname:
                    scene_id = "_".join(fname.split("_")[:-1])
                    grouped[scene_id][band] = os.path.join(folder, fname)
        return list(grouped.values())

    def __len__(self):
        return len(self.patch_coords)

    def __getitem__(self, idx):
        scene_id, y, x = self.patch_coords[idx]

        def read_patch(band_paths):
            patch = []
            for b in ['B04', 'B03', 'B02']:
                with rasterio.open(band_paths[b]) as src:
                    window = rasterio.windows.Window(x, y, self.patch_size, self.patch_size)
                    patch.append(src.read(1, window=window))
            patch = np.stack(patch)
            patch = np.clip(patch / 15000.0, 0, 1)
            return torch.tensor(patch, dtype=torch.float32)

        pre_patch = read_patch(self.pre_scenes[scene_id])
        post_patch = read_patch(self.post_scenes[scene_id])

        return pre_patch, post_patch


def predict_all_scenes_to_mosaic(model_weights_path, dataset, output_dir, device='cpu'):
    # TODO ask why this network is chosen
    model = define_G(net_G='base_transformer_pos_s4_dd8', input_nc=3)
    model = torch.load(model_weights_path, weights_only=False, map_location=torch.device(device))
    model.eval()
    model.to(device)

    os.makedirs(output_dir, exist_ok=True)

    for scene_index, scene in enumerate(dataset.pre_scenes):
        tile = dataset.pre_scenes[0]['B04'].split("_")[-5]
        random_choice = random.choices(string.ascii_letters + string.digits, k=6)
        date_from = dataset.pre_scenes[0]['B04'].split("_")[-4]
        date_to = dataset.post_scenes[0]['B04'].split("_")[-4]

        with rasterio.open(scene['B04']) as ref_src:
            h, w = ref_src.height, ref_src.width
            transform = ref_src.transform
            crs = ref_src.crs

        full_pred = np.zeros((h, w), dtype=np.uint8)
        full_pred_logits = np.zeros((h, w), dtype=np.uint8)

        # Predict patches for this scene only
        for idx, (scene_id, y, x) in enumerate(dataset.patch_coords):
            if scene_id != scene_index:
                continue

            pre_tensor, post_tensor = dataset[idx]
            pre_tensor = pre_tensor.unsqueeze(0).to(device)
            post_tensor = post_tensor.unsqueeze(0).to(device)

            with torch.no_grad():
                output = model(pre_tensor, post_tensor)
                # pred_patch = torch.argmax(output, dim=1).cpu().numpy().astype(np.uint8)
                pred_patch_logits = (
                    torch.softmax(output, dim=1).detach().cpu().numpy()[0, 1, :, :]*100
                ).astype(np.uint8)

            y_shift = pred_patch_logits.shape[0]
            x_shift = pred_patch_logits.shape[1]
            y = min(y, h-y_shift)
            x = min(x, w-x_shift)
            # full_pred[y:y+y_shift, x:x+x_shift] = pred_patch
            full_pred_logits[y:y+y_shift, x:x+x_shift] = pred_patch_logits

        full_pred_logits = full_pred_logits.astype(np.float32)

        # Standardize
        mean_val = np.mean(full_pred_logits)
        std_val = np.std(full_pred_logits)
        std_logits = (full_pred_logits - mean_val) / (std_val + 1e-8)  # avoid division by zero

        # Optionally scale to 0–100 for visualization
        full_pred_logits = (std_logits * 10 + 50).clip(0, 100).astype(np.uint8)

        min_val = full_pred_logits.min()
        max_val = full_pred_logits.max()
        full_pred_logits = (
            (full_pred_logits - min_val) / (max_val - min_val) * 100
        ).astype(np.uint8)

        full_pred[full_pred_logits >= 50] = 1
        full_pred = full_pred.astype(np.uint8)

        # Save individual scene prediction
        # ChDM_S2_20220215_20230316_TJ35_AD6548_pred.tif
        filename_parts = [
            "ChDM_S2",
            date_from,
            date_to,
            tile,
            "".join(random_choice)
        ]
        output_filename = "_".join(filename_parts)

        output_path = os.path.join(
            output_dir,
            output_filename + "_pred.tif")
        with rasterio.open(
            output_path, 'w',
            driver='GTiff',
            height=h, width=w,
            count=1,
            dtype=full_pred.dtype,
            crs=crs,
            transform=transform
        ) as dst:
            dst.write(full_pred, 1)

        output_path_logits = os.path.join(
            output_dir,
            output_filename + "_pred_logits.tif")

        with rasterio.open(
            output_path_logits, 'w',
            driver='GTiff',
            height=h, width=w,
            count=1,
            dtype=full_pred_logits.dtype,
            crs=crs,
            transform=transform
        ) as dst:
            dst.write(full_pred_logits, 1)
    return output_path
