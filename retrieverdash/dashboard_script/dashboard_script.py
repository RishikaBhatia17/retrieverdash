import json
import os
from datetime import datetime, timezone
from json.decoder import JSONDecodeError
from shutil import rmtree
from tempfile import mkdtemp

from retriever import reload_scripts, dataset_names
from retriever import download
from retriever.lib.engine_tools import getmd5

from .status_dashboard_tools import get_dataset_md5
from .status_dashboard_tools import diff_generator
from .status_dashboard_tools import create_dirs
from .status_dashboard_tools import dataset_type

import shutil
import pandas as pd

file_location = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))

IGNORE_LIST = ['alberta-detailed-soil-survey', 'amniote-life-hist', 'antarctic-breed-bird', 'aquatic-animal-excretion', 'arsenic-contamination-bangladesh', 'baltimore-restaurants', 'bioclim', 'biodiversity-response', 'biomass-allometry-db', 'biotime', 'biotimesql', 'bird-migration-data', 'bird-size', 'boston-buildbps', 'boston-buildbpss', 'breast-cancer-wi', 'breed-bird-survey', 'breed-bird-survey-50stop', 'breed-bird-survey-nlcd', 'british-columbia-detailed-soil-survey', 'bupa-liver-disorders', 'butterfly-population-network', 'canada-soil-survery', 'canada-soil-survey', 'catalogos-dados-brasil', 'chytr-disease-distr', 'community-abundance-misc', 'coronavirus-belgium', 'coronavirus-italy', 'coronavirus-south-korea', 'county-emergency-management-offices', 'credit-card-fraud', 'croche-vegetation-data', 'dicerandra-frutescens', 'ecoregions-us', 'fao-global-capture-product', 'felix-riese-hyperspectral-soilmoisture', 'fernow-air-temperature', 'fernow-biomass', 'fernow-forest-streamflow', 'fernow-nadp-rain-chemistry', 'fernow-precipitation', 'fernow-precipitation-chemistry', 'fernow-soil-productivity', 'fernow-stream-chemistry', 'fernow-watershed-acidification', 'fia-alabama', 'fia-alaska', 'fia-american-samoa', 'fia-arizona', 'fia-arkansas', 'fia-california', 'fia-colorado', 'fia-connecticut', 'fia-delaware', 'fia-federated-states-micrones', 'fia-florida', 'fia-georgia', 'fia-guam', 'fia-hawaii', 'fia-idaho', 'fia-illinois', 'fia-indiana', 'fia-iowa', 'fia-kansas', 'fia-kentucky', 'fia-louisiana', 'fia-maine', 'fia-maryland', 'fia-massachusetts', 'fia-michigan', 'fia-minnesota', 'fia-mississippi', 'fia-missouri', 'fia-montana', 'fia-nebraska', 'fia-nevada', 'fia-new-hampshire', 'fia-new-jersey', 'fia-new-mexico', 'fia-new-york', 'fia-north-carolina', 'fia-north-dakota', 'fia-northern-mariana-islands', 'fia-ohio', 'fia-oklahoma', 'fia-oregon', 'fia-palau', 'fia-pennsylvania', 'fia-puerto-rico', 'fia-rhode-island', 'fia-south-carolina', 'fia-south-dakota', 'fia-tennessee', 'fia-texas', 'fia-us-virgin-islands', 'fia-utah', 'fia-vermont', 'fia-virginia', 'fia-washington', 'fia-west-virginia', 'fia-wisconsin', 'fia-wyoming', 'fish-parasite-hosts', 'flensburg-food-web', 'foreign-exchange-rates-2000-2019', 'forest-biomass-china', 'forest-fires-portugal', 'forest-inventory-analysis', 'forest-plots-michigan', 'forest-plots-wghats', 'foster-ltreb', 'fray-jorge-ecology', 'gdp', 'gentry-forest-transects', 'global-population-dynamics', 'globi-interaction', 'great-basin-mammal-abundance', 'harvard-forest', 'home-ranges', 'intertidal-abund-me', 'jornada-lter-rodent', 'la-selva-trees', 'lake-county-illinois-cancer-rates', 'lakecats-final-tables', 'leaf-herbivory', 'macroalgal-communities', 'macrocystis-variation', 'mammal-community-db', 'mammal-diet', 'mammal-life-hist', 'mammal-masses', 'mammal-metabolic-rate', 'mammal-super-tree', 'mapped-plant-quads-co', 'mapped-plant-quads-id', 'mapped-plant-quads-ks', 'mapped-plant-quads-mt', 'marine-recruitment-data', 'mediter-basin-plant-traits', 'mt-st-helens-veg', 'nadp-precipitation-chemistry', 'national-pedon-database-summary-layer','nematode-traits', 'new-york-city-airbnb-open-data', 'ngreatplains-flowering-dates', 'nla', 'noaa-fisheries-trade', 'north-carolina-piedmont-mapped-foreset', 'north-carolina-piedmont-permanent-plots', 'north-carolina-piedmont-seedlng-sampling', 'north-carolina-piedmont_seedlng_sampling', 'nova-scotia-detailed-soil-survey', 'npn','nyc-tree-count', 'ontario-detailed-soil-survey', 'pantheria', 'partners-in-flight', 'phytoplankton-size', 'plant-comp-ok', 'plant-life-hist-eu', 'plant-occur-oosting', 'plant-taxonomy-us', 'portal-project-teaching', 'prairie-forest', 'predator-prey-body-ratio', 'predator-prey-size-marine', 'prince-edward-island-detailed-soil-survey', 'prism-climate', 'predicts', 'rainfall-in-india', 'sample-hdf', 'saskatchewan-detailed-soil-survey', 'shortgrass-steppe-lter', 'socean-diet-data', 'soil-db', 'soil-landscapes-of-canada', 'sonoran-desert', 'species-exctinction-rates', 'streamflow-conditions', 'sycamore-creek-macroinvertebrate', 'titanic', 'transparencia-dados-abertos-brasil', 'tree-canopy-geometries', 'tree-demog-wghats', 'turtle-offspring-nesting', 'usda-agriculture-plants-database', 'usda-dietary-supplement-ingredient-data', 'usda-mafcl-fooddatacenteral-alldatatypes', 'usda-mafcl-fooddatacenteral-brandedfoods', 'usda-mafcl-fooddatacenteral-fndds', 'usda-mafcl-fooddatacenteral-foundationfoods', 'usda-mafcl-fooddatacenteral-srlegacy', 'usda-mafcl-fooddatacenteral-supportingdata', 'usda-mafcl-standard-reference', 'usgs-elevation', 'ushio-maizuru-fish-community', 'veg-plots-sdl', 'vertnet', 'vertnet-amphibians', 'vertnet-birds', 'vertnet-fishes', 'vertnet-mammals', 'vertnet-reptiles', 'virgin-islands-coral-decadal-scale', 'virgin-islands-coral-diadema-antillarum', 'virgin-islands-coral-geography', 'virgin-islands-coral-juvenile', 'virgin-islands-coral-landscape-scale', 'virgin-islands-coral-octocorals-count', 'virgin-islands-coral-physical-measurements', 'virgin-islands-coral-population-projections', 'virgin-islands-coral-recruitment-tiles', 'virgin-islands-coral-scleractinian-corals', 'virgin-islands-coral-taxonomy', 'virgin-islands-coral-yawzi-transects', 'white-clay-creek-avondale-soil', 'white-clay-creek-boulton-chemistry', 'white-clay-creek-chlorophyll', 'white-clay-creek-christina-chemistry', 'white-clay-creek-christina-sediment', 'white-clay-creek-christina-temperatures', 'white-clay-creek-streamflow', 'white-clay-creek-swrc-meteorology', 'white-clay-creek-waterlevels', 'white-clay-dissolved-carbon', 'white-clay-dissolved-carbon ', 'wine-composition', 'wine-quality', 'wood-density', 'worldclim-five', 'worldclim-ten', 'worldclim-thirty', 'worldclim-twofive', 'yukon-detailed-soil-survey', 'zipcodes']

def check_dataset(dataset):
    md5 = None
    status = None
    reason = None
    diff = None
    dataset_detail = None
    try:
        try:
            with open(os.path.join(file_location, "dataset_details.json"), 'r') as json_file:
                dataset_detail = json.load(json_file)
        except (OSError, JSONDecodeError):
            dataset_detail = dict()
            dataset_detail['dataset_details'] = {}

        if dataset_type(dataset) == 'spatial':
            workdir = None
            try:
                workdir = mkdtemp(dir=file_location)
                download(dataset.name, path=workdir)
                md5 = getmd5(workdir, data_type='dir')
            finally:
                if workdir:
                    rmtree(workdir)
        else:
            md5 = get_dataset_md5(dataset)
            if dataset.name not in dataset_detail \
                    or md5 != dataset_detail[dataset.name]['md5']:
                diff = diff_generator(dataset)
        status = True
    except Exception as e:
        reason = str(e)
        status = False
    finally:
        json_file_details = dataset_detail
        json_file_details["dataset_details"][dataset.name] = {
            "md5": md5,
            "status": status,
            "reason": reason,
            "diff": diff}
        json_file_details["last_checked_on"] = datetime.now(timezone.utc).strftime("%d %b %Y")
        dataset_details_write = open(os.path.join(file_location, 'dataset_details.json'), 'w')
        json.dump(json_file_details, dataset_details_write,
                  sort_keys=True, indent=4)
        dataset_details_write.close()

def viewcsvfiles():
    #STEP 1: Creating "csvfiles" folder in dashboard_script folder.

    script_directory = os.path.dirname(__file__) # directory of script
    csv_files_dir = r'{}/csvfiles'.format(script_directory) # path to be created

    try:
        os.makedirs(csv_files_dir) #creating the directory
    except OSError:
        pass

    #STEP 2: Copying all csv files from subfolders in "old" folder to "csvfiles" folder 

    oldfolder = r'{}/old'.format(script_directory) #dstpath
    csvfilefolder = r'{}/csvfiles'.format(script_directory) #path
    for root, dirs, files in os.walk((os.path.normpath(oldfolder)), topdown=False):
            for name in files:
                if name.endswith('.csv'):
                    SourceFolder = os.path.join(root,name)
                    shutil.copy2(SourceFolder, csvfilefolder) #copies csv to new folder
    print ("Copied all csv files")

    # STEP3: Convert CSV files to HTML Tables and store them in "old" folder to replicate "diff" folder and column
    
    os.chdir(csvfilefolder)

    # iterate through all file
    for file in os.listdir():
        # Check whether file is in csv format or not
        if file.endswith(".csv"):
            file_path = f"{csvfilefolder}/{file}"
            #print(file_path)
            html_file_name = os.path.splitext(file)[0]
            #print(html_file_name)
            a = pd.read_csv(file_path)
            html_file = a.to_html()
            #write html to file
            os.chdir(oldfolder)
            text_file = open(html_file_name + ".html", "w")
            text_file.write(html_file)
            text_file.close()

    shutil.rmtree(csvfilefolder)
        
def run():
    create_dirs()
    datasets_to_check = [script for script in reload_scripts() if
                         script.name not in IGNORE_LIST]
    for dataset in datasets_to_check:
        check_dataset(dataset)
    viewcsvfiles()

if __name__ == '__main__':
    run()
