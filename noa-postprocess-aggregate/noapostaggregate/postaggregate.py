import os
import xarray as xr
import rasterio as rio
import rioxarray


class Aggregate:

    def __init__(self, path, config_file) -> None:
        self._path = path
        pass

    def from_path(self, agg_function):
        images, profile = self._get_images_and_profile()
        median_img = self._get_median_img(images, 3)

        print("==========================================")
        # print(profile)
        # print(median_img)
        # print("==========================================")
        profile.update(
                driver="GTiff",
                width=median_img.shape[2],
                height=median_img.shape[1]
            )
        with rio.Env():
            with rio.open('./output/medians.tif', 'w', **profile) as dst:
                dst.write(median_img.astype(rio.uint8))
                # dst.write(median_img.asarray(rio.uint8))

    def _get_images_and_profile(self):
        tci_images = []
        profile = None
        for root, dirs, files in os.walk(self._path, topdown=True):
            for fname in files:
                if fname.endswith(".tif"):
                    that_image = rio.open(os.path.join(root, fname))
                    if profile is None:
                        temp_image = rio.open(os.path.join(root, fname))
                        profile = temp_image.profile
                    print(that_image.profile)
                    tci_images.append(rioxarray.open_rasterio(os.path.join(root, fname), chunks={'x': 2000, 'y': 2000}))
        return (tci_images, profile)

    def _get_median_img(self, imgs, no_of_bands):

        bands_medians = []
        for b in range(no_of_bands):
            bands = [img.sel(band=b+1).astype(rio.uint8) for img in imgs]
            bands_comp = xr.concat(bands, dim='band')
            bands_medians.append(bands_comp.median(dim='band', skipna=True))
            print(f"Band: {b}, comp: {bands_medians[b]}")
        return xr.concat(bands_medians, dim='band')
