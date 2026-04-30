import requests, json, ee
import shapely
import pandas as pd
import geopandas as gpd

ee.Initialize()
from city_metrix.metrix_model import GeoExtent

PCTL_FULLVEG = 75
PCTL_NONVEG = 5
dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
S2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
S2CS = ee.ImageCollection("GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED")
CLEAR_THRESHOLD = 0.60

def get_endmembers(aoi, year=2025, vegpctl=None, soilpctl=None):

    if vegpctl is None:
        vegpctl = PCTL_FULLVEG
    if soilpctl is None:
        soilpctl = PCTL_NONVEG

    date_start, date_end = f'{year}-01-01', f'{year+1}-01-01'

    # Cloud score+
    S2filtered = (
        S2.filterBounds(aoi)
        .filterDate(date_start, date_end)
        .linkCollection(S2CS, ["cs"])
        .map(lambda img: img.updateMask(img.select("cs").gte(CLEAR_THRESHOLD)).divide(10000))
    )

    # Function to add NDVI to Sentinel images
    def addNDVI(image):
        ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI")
        return image.addBands(ndvi)

    S2ndvi = S2filtered.map(addNDVI)

    # Create a 90th percentile NDVI image
    ndvi = S2ndvi.select("NDVI").reduce(ee.Reducer.percentile([90])).rename("NDVI")

    # Filter dynamic world data
    dwFiltered = dw.filterBounds(aoi).filterDate(date_start, date_end)

    # choose the most commonly occurring class for each pixel
    # clip to city area and map to 1 for veg (trees & grass)
    # and 2 for bare soil
    dwMode = dwFiltered.select("label").reduce(ee.Reducer.mode()).clip(aoi)

    dwClass = (
        dwMode.remap([0, 1, 2, 3, 4, 5, 6, 7, 8], [0, 1, 1, 0, 0, 1, 2, 0, 0])
        .selfMask()
        .rename("lc")
    )

    # Percentile values from:
    # Zeng, X., Dickinson, R. E., Walker, A., Shaikh, M., DeFries, R. S., & Qi, J. (2000).
    # Derivation and Evaluation of Global 1-km Fractional Vegetation Cover Data for Land Modeling.
    # Journal of Applied Meteorology, 39(6), 826–839.
    # https://doi.org/10.1175/1520-0450(2000)039<0826:DAEOGK>2.0.CO;2

    # Calculates the nth percentile value for vegetation and soil NDVI

    vegNDVI = (
        ndvi.updateMask(dwClass.eq(1))
        .reduceRegion(
            reducer=ee.Reducer.percentile([vegpctl]),
            geometry=aoi,
            scale=10,
            maxPixels=10e13,
        )
        .get("NDVI")
        .getInfo()
    )
    soilNDVI = (
        ndvi.updateMask(dwClass.eq(2))
        .reduceRegion(
            reducer=ee.Reducer.percentile([soilpctl]),
            geometry=aoi,
            scale=10,
            maxPixels=10e13,
        )
        .get("NDVI")
        .getInfo()
    )

    return vegNDVI, soilNDVI

TARGET_CITIES = [
    # "IND-Jabalpur",
    # "IND-Indore",
    # "IND-Gwalior",
    # "USA-Atlanta",
    # "USA-Boston",
    # "GBR-London",
    # "ESP-Barcelona",
    # "KEN-Kisumu",
    "NLD-Amsterdam"
]

CITYDATA_URL = 'https://dev.cities-data-api.wri.org/cities'
citydata = requests.get(CITYDATA_URL).json()
cities = [c for c in citydata['cities'] if c['id'] in TARGET_CITIES]

results = []
for city in cities:
    zone_json = json.dumps({"city_id": city['id'], "aoi_id": "urban_extent"})
    bbox = GeoExtent(zone_json)
    bbox_ee = bbox.to_ee_rectangle()["ee_geometry"]
    vegNDVI, soilNDVI = get_endmembers(bbox_ee)
    res = gpd.GeoDataFrame({
        "city_id": [city['id']],
        "year": [2025],
        "vegNDVI": [vegNDVI],
        "soilNDVI": [soilNDVI],
        "geometry": shapely.geometry.box(*bbox.coords)
        })
    results.append(res)

endmember_table = pd.concat(results)
endmember_table.to_file("endmembers_dataforcoolcities_2.geojson", driver="GEOJSON")