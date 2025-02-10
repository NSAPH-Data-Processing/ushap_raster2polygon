import logging
import os
import zipfile
import hydra
import wget

# Function to get the URL for a given year
def get_url(base_url, year):
    return base_url.format(year=year)

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    url = cfg.ushap[cfg.temporal_freq].base_url.format(year=cfg.year)
    tgt_path = f"data/input/ushap/{cfg.temporal_freq}"

    if cfg.temporal_freq == "daily" or cfg.temporal_freq == "monthly":
        tgt_file = f"{tgt_path}/ushap_{cfg.year}.zip"

        logging.info(f"Downloading {url}")
        wget.download(url, tgt_file)
        logging.info(f"Done.")

        # unzip 
        with zipfile.ZipFile(tgt_file, "r") as zip_ref:
            zip_ref.extractall(f"{tgt_path}/{cfg.year}") if cfg.temporal_freq == "monthly" else zip_ref.extractall(tgt_path)
        logging.info(f"Unzipped zip for year {cfg.year}")

    elif cfg.temporal_freq == "yearly":
        logging.info(f"Downloading {url}")
        # Construct the base path, while handling potential symlinks
        real_path = os.path.realpath(tgt_path)
        os.makedirs(os.path.join(real_path, cfg.year), exist_ok=True)
        wget.download(url, f"{tgt_path}/{cfg.year}/")
        #year converted to a string (alterative approach when temppral_freq is yearly)
        #os.makedirs(os.path.join(real_path, str(cfg.year)), exist_ok=True) 
        #wget.download(url, f"{tgt_path}/{str(cfg.year)}/") 
        logging.info(f"Done.")

if __name__ == "__main__":
    main()