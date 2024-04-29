import logging
import hydra
import geopandas as gpd
import pickle

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    shp_file = f"data/input/shapefiles/shapefile_{cfg.polygon_name}_{cfg.shapefile_year}/shapefile.shp"
    pkl_file = f"data/intermediate/pkl_shapefiles/shapefile_{cfg.polygon_name}_{cfg.shapefile_year}.pkl"

    # read the shapefile
    logging.info(f"Reading shapefile...")
    shp = gpd.read_file(shp_file)
    shp.geometry = shp.geometry.simplify(0.001, preserve_topology=True)

    # save the pickle
    logging.info(f"Saving shapefile (pickle)...")
    with open(pkl_file, "wb") as f:
        pickle.dump(shp, f, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    main()