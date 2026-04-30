import requests, json,  ee
from city_metrix.metrix_model import GeoExtent
ee.Initialize(project="wri-gee")

BUCKET_NAME = 'wri-cities-indicators'
PREFIX = 'devdata/inputdata/isochrones'
DEFAULT_SPATIAL_RESOLUTION = 100

TARGET_CITIES = [
    # "IND-Jabalpur",
    # "IND-Indore",
    # "IND-Gwalior",
    # "USA-Atlanta",
    # "USA-Boston",
    # "GBR-London",
    # "ESP-Barcelona",
    # "KEN-Kisumu"
    "NLD-Amsterdam"
]

CITYDATA_URL = 'https://dev.cities-data-api.wri.org/cities'
citydata = requests.get(CITYDATA_URL).json()
cities = [c for c in citydata['cities'] if c['id'] in TARGET_CITIES]

for city in cities:
    city_id = city['id']
    try:
        city_dict = {"city_id": city_id, "aoi_id": "urban_extent"}
        city_json = json.dumps(city_dict)
        bbox_ee = GeoExtent(city_json).to_ee_rectangle()['ee_geometry']
        bbox_feat = ee.Feature(bbox_ee, {'city_id': city_id, 'aoi_id': 'urban_extent', 'version':'devapi_2026-03-05'})
        bbox_list = bbox_list.add(bbox_feat)
    except:
        print(f"Error: {city_id}")

output_fc = ee.FeatureCollection(bbox_list)

task = ee.batch.Export.table.toAsset(
      collection = output_fc,
      description = 'bboxes',
      assetId = 'projects/wri-datalab/cities/bboxes/cif_bboxes_3',
  )
task.start()