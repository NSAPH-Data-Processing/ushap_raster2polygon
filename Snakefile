import yaml
from src.aggregate_ushap import available_shapefile_year

conda: "requirements.yaml"
configfile: "conf/config.yaml"
envvars:
    "PYTHONPATH"  # this indicates that the PYTHONPATH must be set, always done in docker

# == Load configuration ==
# fixed config files
ushap_cfg = yaml.safe_load(open(f"conf/ushap.yaml", 'r'))
# dynamic config files
defaults_dict = {key: value for d in config['defaults'] if isinstance(d, dict) for key, value in d.items()}
shapefiles_cfg = yaml.safe_load(open(f"conf/shapefiles/{defaults_dict['shapefiles']}.yaml", 'r'))
# == Define variables ==
polygon_name = config["polygon_name"]
print(f"polygon_name: {polygon_name}")
temporal_freq = config["temporal_freq"]
print(f"temporal_freq: {temporal_freq}")
shapefile_years_list = list(shapefiles_cfg[polygon_name].keys())
print(f"shapefile_years_list: {shapefile_years_list}")
years_list = list(range(2000, 2020+1))
print(f"years_list: {years_list}")

# == Define rules == 
rule all:
    input:
        expand(
            f"data/output/ushap_raster2polygon/{polygon_name}_{temporal_freq}/pm25__ushap__{polygon_name}_{temporal_freq}__{{year}}.parquet",
            year=years_list
        )

rule download_shapefiles:
    output:
        f"data/input/shapefiles/shapefile_{polygon_name}_{{shapefile_year}}/shapefile.shp" 
    shell:
        f"""
        python src/download_shapefile.py polygon_name={polygon_name} shapefile_year={{wildcards.shapefile_year}}
        """

rule pkl_shapefiles:
    input:
        f"data/input/shapefiles/shapefile_{polygon_name}_{{shapefile_year}}/shapefile.shp"
    output:
        f"data/intermediate/pkl_shapefiles/shapefile_{polygon_name}_{{shapefile_year}}.pkl"
    shell:
        f"""
        python src/shp2pkl.py polygon_name={polygon_name} shapefile_year={{wildcards.shapefile_year}}
        """

rule download_ushap:
    output:
        f"data/input/ushap/{temporal_freq}/{{year}}/{ushap_cfg[temporal_freq]['file_name']}" if temporal_freq == "yearly" else f"data/input/ushap/{temporal_freq}/ushap_{{year}}.zip"
    params:
        year="{year}"
    shell:
        f"""
        python src/download_ushap.py temporal_freq={temporal_freq} year={{params.year}}
        """

def get_pkl_path(wildcards):
    shapefile_year = available_shapefile_year(int(wildcards.year), shapefile_years_list)
    return f"data/intermediate/pkl_shapefiles/shapefile_{polygon_name}_{shapefile_year}.pkl"

def get_ushap_paths(wildcards):
    base_path = f"data/input/ushap/{temporal_freq}/{wildcards.year}"
    if temporal_freq == "yearly":
        return [f"{base_path}/{ushap_cfg[temporal_freq]['file_name'].format(year=wildcards.year)}"]
    else:
        return [f"data/input/ushap/{temporal_freq}/ushap_{wildcards.year}.zip"]

rule aggregate_ushap:
    input:
        get_pkl_path,
        get_ushap_paths
    output:
        f"data/output/ushap_raster2polygon/{polygon_name}_{temporal_freq}/pm25__ushap__{polygon_name}_{temporal_freq}__{{year}}.parquet"
    shell:
        f"""
        PYTHONPATH=. python src/aggregate_ushap.py polygon_name={polygon_name} temporal_freq={temporal_freq} year={{wildcards.year}} 
        """
