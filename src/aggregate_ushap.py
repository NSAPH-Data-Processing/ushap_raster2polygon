import xarray
import rasterio
import pandas as pd
import geopandas as gpd
import numpy as np
import pickle
from tqdm import tqdm
import matplotlib.pyplot as plt
import os
from datetime import datetime
import hydra
import logging
from utils.faster_zonal_stats import polygon_to_raster_cells

# configure logger to print at info level
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def available_shapefile_year(year, shapefile_years_list: list):
    """
    Given a list of shapefile years,
    return the latest year in the shapefile_years_list that is less than or equal to the given year
    """
    for shapefile_year in sorted(shapefile_years_list, reverse=True):
        if year >= shapefile_year:
            return shapefile_year

    return min(
        shapefile_years_list
    )  # Returns the last element if year is greater than the last element


@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    # load shapefile
    LOGGER.info("Loading shapefile...")
    shapefile_years_list = list(cfg.shapefiles[cfg.polygon_name].keys())
    shapefile_year = available_shapefile_year(cfg.year, shapefile_years_list)

    shape_path = f"data/intermediate/pkl_shapefiles/shapefile_{cfg.polygon_name}_{shapefile_year}.pkl"
    with open(shape_path, "rb") as f:
        polygon = pickle.load(f)
    polygon_ids = polygon[cfg.shapefiles[cfg.polygon_name][shapefile_year].idvar].values

    if cfg.temporal_freq == "daily":
        file_name = cfg.ushap.daily.file_name.format(year = cfg.year, month = "01", day = "01")
    elif cfg.temporal_freq == "monthly":
        file_name = cfg.ushap.monthly.file_name.format(year = cfg.year, month = "01")
    elif cfg.temporal_freq == "yearly":
        file_name = cfg.ushap.yearly.file_name.format(year = cfg.year)
    
    raster_path = f"data/input/ushap/{cfg.temporal_freq}/{cfg.year}/{file_name}"
    ds = xarray.open_dataset(raster_path)
    layer = ds[cfg.ushap.layer]

    # longitude/latitude info used for affine transform
    lon = layer[cfg.ushap.longitude_dim].values
    lat = layer[cfg.ushap.latitude_dim].values
    dlon = (lon[1] - lon[0]) #/ cfg.downscaling_factor
    dlat = (lat[0] - lat[1]) #/ cfg.downscaling_factor

    # first time computing mapping from vector geometries to raster cells
    LOGGER.info("Mapping polygons to raster cells...")
    x = layer.values.astype(np.float32)  # 32-bit improves memory after downscaling
    transform = rasterio.transform.from_origin(lon[0], lat[0], dlon, dlat)
    poly2cells = polygon_to_raster_cells(
        polygon.geometry.values,
        x,
        affine=transform,
        all_touched=True,
        nodata=np.nan,
        verbose=True #cfg.show_progress,
    )

    if cfg.temporal_freq == "yearly":
        stats = []
        for indices in poly2cells:
            if len(indices[0]) == 0:
                # no cells found for this polygon
                stats.append(np.nan)
            else:
                cells = x[indices]
                if sum(~np.isnan(cells)) == 0:
                    # no valid cells found for this polygon
                    stats.append(np.nan)
                    continue
                else:
                    # compute mean of valid cells
                    stats.append(np.nanmean(cells))

        df = pd.DataFrame(
            {"year": cfg.year, cfg.ushap.layer: stats},
            index=pd.Index(polygon_ids, name=cfg.polygon_name),
        )

        if cfg.plot_output:
            # convert to geopandas for image
            gdf = gpd.GeoDataFrame(
                        df, geometry=polygon.geometry.values, crs=polygon.crs
                    )
            png_path = f"logs/ushap_{cfg.polygon_name}_{cfg.year}.png"
            gdf.plot(column=cfg.ushap.layer, legend=True)
            plt.savefig(png_path)
            LOGGER.info("Plotted result.")

    if cfg.temporal_freq == "daily" or cfg.temporal_freq == "monthly":
        df_chunks = []  # collects the results
        LOGGER.info("Computing zonal stats for each period")
        files = [file for file in os.listdir(f"data/input/ushap/{cfg.temporal_freq}/{cfg.year}/")]
        files = sorted(files)

        for i, file in tqdm(enumerate(files)): 
            stats = []
            raster_path = f"data/input/ushap/{cfg.temporal_freq}/{cfg.year}/{file}"
            
            ds = xarray.open_dataset(raster_path)
            layer = ds[cfg.ushap.layer]
            x = layer.values.astype(np.float32)  # reference array

            for indices in poly2cells:
                if len(indices[0]) == 0:
                    # no cells found for this polygon
                    stats.append(np.nan)
                else:
                    cells = x[indices]
                    if sum(~np.isnan(cells)) == 0:
                        # no valid cells found for this polygon
                        stats.append(np.nan)
                        continue
                    else:
                        # compute mean of valid cells
                        stats.append(np.nanmean(cells))

            date_str = file.split('_')[3]
            if cfg.temporal_freq == "daily":
                date = datetime.strptime(date_str, '%Y%m%d').date()
                df = pd.DataFrame(
                    {"date": date, cfg.ushap.layer: stats},
                    index=pd.Index(polygon_ids, name=cfg.polygon_name)
                )
            elif cfg.temporal_freq == "monthly":
                date = datetime.strptime(date_str, '%Y%m').date()
                df = pd.DataFrame(
                    {"year": date.year, "month": date.month, cfg.ushap.layer: stats},
                    index=pd.Index(polygon_ids, name=cfg.polygon_name)
                )   

            df_chunks.append(df)

            if i == 0 and cfg.plot_output:
                # convert to geopandas for image
                gdf = gpd.GeoDataFrame(
                            df, geometry=polygon.geometry.values, crs=polygon.crs
                        )
                png_path = f"logs/ushap_{cfg.polygon_name}_{date_str}.png"
                gdf.plot(column=cfg.ushap.layer, legend=True)
                plt.savefig(png_path)
                LOGGER.info("Plotted result.")
        # concatenate all periods
        df = pd.concat(df_chunks)

    # == save output file
    # Construct the base path, while handling potential symlinks
    base_path = f"data/output/ushap_raster2polygon/{cfg.temporal_freq}"
    real_path = os.path.realpath(base_path)
    full_path = os.path.join(real_path, cfg.polygon_name)
    os.makedirs(full_path, exist_ok=True)
    output_file = f"{full_path}/ushap_{cfg.year}.parquet"
    
    LOGGER.info(f"Saving output to {output_file}")
    df.to_parquet(output_file)

if __name__ == "__main__":
    main()
