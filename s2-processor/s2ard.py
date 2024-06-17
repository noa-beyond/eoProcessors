import os
from osgeo import gdal
import numpy as np
import xml.etree.ElementTree as ET
import sys

class s2Processor():

    def __init__(self, product, in_dir, out_dir, config_dict):

        # read in configuration settings
        self.config = config_dict
        self.input_dir = in_dir
        self.tiles = self.get_tiles(product)
        self.product = product
        self.output_dir = out_dir
        self.bands_to_keep = ['B02_10m', 'B03_10m', 'B04_10m', 'B08_10m', 'TCI_10m','B01_20m','B05_20m', 'B06_20m', 'B07_20m', 'B8A_20m', 'B11_20m', 'B12_20m','SCL_20m','B09_60m']
    
    # the following function get the tile name of a Sentinel-2 product based on the folder name
    def get_tiles(self,product):
        return product.split('_')[5]

    def get_date(self, product):
        date = product.split('_')[2]
        return date[:4], date[4:6], date[6:8]

    def _get_boa_band_pathes(self):
        band_pathes = {}
        metadata_xml = os.path.join(self.input_dir, self.product, 'MTD_MSIL2A.xml')
        root = ET.parse(metadata_xml)
        product = root.findall('.//Product_Organisation/Granule_List/Granule')
        for res_dir in product:
            for band in res_dir.findall('IMAGE_FILE'):
                band_pathes[band.text[-7:]] = os.path.split(metadata_xml)[0] + os.sep + band.text + '.jp2'
        print(band_pathes)
        return(band_pathes)


    def process_tile(self):
        # # FMASK CLOUD MASKING
        # if (self.config.ard_settings['cloud-mask'] == True) and (self.config.cloud_mask_settings['fmask-codes']) and (producttype == 'L1C'):
        #     print('RUNNING FMASK CLOUD MASK')
        #     # running fmask cloud masking
        #     fmask_image = work_dir + os.sep + '_'.join([os.path.splitext(os.path.split(input_tile)[1])[0], 'FMASK']) + '.tif'
        #     system_command = ['fmask_sentinel2Stacked.py', '-o', fmask_image, '--safedir', input_tile]
        #     system_call(system_command)

        #     # copying fmask image to output dir
        #     output_image = self.rename_image(self.output_dir, '.tif', os.path.split(os.path.splitext(fmask_image)[0])[1])
        #     copyfile(fmask_image, output_image)

        #     # resampling to target resolution if bands/image does not meet target resolution
        #     if rm.get_band_meta(fmask_image)['geotransform'][1] != self.image_properties['resolution']:
        #         # changing resampling to near since cloud mask image contains discrete values
        #         _image_properties = self.image_properties.copy()
        #         _image_properties["resampling_method"] = "near"
        #         fmask_image = rm.resample_image(fmask_image, 'resampled', _image_properties)

        #     # applying fmask as mask to ref images
        #     print('APPLYING FMASK CLOUD MASK')
        #     mask = rm.binary_mask(rm.read_band(fmask_image), self.config.cloud_mask_settings['fmask-codes'])
        #     for key in self.bands:
        #         band_meta = rm.get_band_meta(ref_bands[key])
        #         masked_array = rm.mask_array(mask, rm.read_band(ref_bands[key]))
        #         masked_image = self.rename_image(work_dir, '.tif', os.path.splitext(os.path.basename(ref_bands[key]))[0], 'fmask', 'masked')
        #         rm.write_image(masked_image, "GTiff", band_meta, [masked_array])
        #         ref_bands[key] = masked_image

        #     # apply fmask as mask to index images
        #     print('APPLYING FMASK CLOUD MASK TO INDICES')
        #     if self.derived_indices != False:
        #         for key in self.derived_indices:
        #             band_meta = rm.get_band_meta(derived_bands[key])
        #             masked_array = rm.mask_array(mask, rm.read_band(derived_bands[key]))
        #             masked_image = self.rename_image(work_dir, '.tif', os.path.splitext(os.path.basename(derived_bands[key]))[0], 'fmask', 'masked')
        #             rm.write_image(masked_image, "GTiff", band_meta, [masked_array])
        #             derived_bands[key] = masked_image



        band_paths = self._get_boa_band_pathes()

        for band,path in band_paths.items():
            if band not in self.bands_to_keep:
                continue
            raw_product = os.path.join(self.input_dir, self.product, path)
            print(f"Processing {raw_product}")
            if not os.path.exists(raw_product):
                print(f"File {raw_product} does not exist")
                continue
            tile = self.get_tiles(self.product)
            year,month,day  = self.get_date(product)
            os.makedirs(os.path.join(self.output_dir, tile, year, month, day), exist_ok=True)
            new_product = os.path.join(self.output_dir,tile,year,month,day)
            new_product = os.path.join(new_product, f"{tile}_{year}{month}{day}_{band.split('_')[0]}.tif")
            # check if new product already exists
            if os.path.exists(new_product):
                print(f"File {new_product} already exists")
                continue
            cmd = f'gdalwarp -t_srs EPSG:3857 -co COMPRESS=DEFLATE -co TILED=True -co PREDICTOR=2 -co NUM_THREADS=ALL_CPUS -tr 10 10 {raw_product} {new_product}'
            os.system(cmd)

  

if __name__ == "__main__":
    # # parse command line arguments
    # desc = "Sentinel-2 Analysis Ready Data"
    # parser = ArgumentParser(description=desc)
    # parser.add_argument("--tiles", "-t", type=str, dest='tiles', help="Sentinel-2 data product name", required=True)
    # args = parser.parse_args()

    # # working directories
    # work_dir = "/work"
    # output_dir = "/output"
    # mosaic_dir = "/output/mosaic"
    # average_dir = "/output/average"
    # os.makedirs(mosaic_dir, exist_ok=True)

    # config_file = os.path.join(work_dir, 'config.yml')

    # get input directory from docker run command parameter

     # # geojson
    # aoi_file = os.path.dirname(os.path.realpath(__file__)) + os.sep + 'aoi.geojson'

    work_dir = '/usr/src/app/input'
    output_dir = '/usr/src/app/output'

    for product in os.listdir(work_dir):
        if product.endswith('.SAFE') and product.split('_')[1] == 'MSIL2A':
            processor = s2Processor(product, work_dir, output_dir, None)    
            processor.process_tile()
