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
    url = cfg.ushap[cfg.temporal_freq].base_url.format(year=cfg.year) #TODO replace with config entry cfg.ushap.url
    tgt = f"data/input/ushap/{cfg.temporal_freq}" #TODO replace daily with config

    if cfg.temporal_freq == "daily" or cfg.temporal_freq == "monthly":
        logging.info(f"Downloading {url}")
        wget.download(url, f"{tgt}/zipfile.zip")
        logging.info(f"Done.")

        # unzip 
        with zipfile.ZipFile(f"{tgt}/zipfile.zip", "r") as zip_ref:
            zip_ref.extractall(f"{tgt}/{cfg.year}")
        logging.info(f"Unzipped zip for year {cfg.year}")

        # remove zip file
        os.remove(f"{tgt}/zipfile.zip")
        logging.info(f"Removed zip")

    elif cfg.temporal_freq == "yearly":
        logging.info(f"Downloading {url}")
        os.makedirs(f"{tgt}/{cfg.year}", exist_ok=True)
        wget.download(url, f"{tgt}/{cfg.year}/")
        logging.info(f"Done.")

if __name__ == "__main__":
    main()