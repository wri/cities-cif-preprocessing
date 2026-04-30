import os, sys, requests, json, pathlib, datetime, shapely, boto3, dask
from dask.distributed import print as dprint
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import r5py
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_points_griddata, rasterize_points_radial
from botocore.exceptions import ClientError

from city_metrix.layers import WorldPop
from city_metrix.metrix_model import GeoExtent, GeoZone

'''
This script takes a locally stored amenity-points GDF (stored as geojson) and calculates a distance-to-amenity
raster in meters using the WorldPop grid. The resulting raster is stored locally and in an S3 bucket.
The script requires a locally stored PBF file of the local road network.
'''

AMENITY_NAME = 'subways'
LOCAL_AMENITYPOINTS_PREFIX = 'C:/Users/tgwon/wri/indicators/demodata/amenitypoints'
LOCAL_PBF_PREFIX = 'C:/Users/tgwon/wri/indicators/pbf'

SESSION = boto3.Session(profile_name='CitiesUserPermissionSet-540362055257')
BUCKET = 'wri-cities-indicators'
LOCAL_DISTANCERASTER_PREFIX = 'C:/Users/tgwon/wri/indicators'
S3_DISTANCERASTER_PREFIX = 'devdata/inputdata/distancerasters'

CITYDATA_URL = 'https://dev.cities-data-api.wri.org/cities'
citydata = requests.get(CITYDATA_URL).json()
focal_cities = [c for c in citydata['cities'] if c['id']=='CHN-Chengdu']
# focal_cities = [c for c in citydata['cities'] if 'dataforcoolcities' in c['projects']]



MAX_DISTANCE = 5000 # Furthest possible travel
GRID_SIZE = 1000 # For divvying up pop rasters
TRAVEL_MODES = {'walk': r5py.TransportMode.WALK}
TRAVEL_SPEEDS = {'walk': 60}

def upload_s3(session, file_name, bucket, object_name):
    s3_client = session.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={'ACL': 'public-read'})
    except ClientError as e:
        logging.error(e)
        return False
    return True

def buffered_bbox_as_geog(bbox_wsen, buffer_distance_meters):
    bbox = GeoExtent(bbox_wsen)
    utm_crs = bbox.as_utm_bbox().crs
    bbox_utm = bbox.as_utm_bbox()
    buffered_utm = [
        bbox_utm.bbox[0] - buffer_distance_meters,
        bbox_utm.bbox[1] - buffer_distance_meters,
        bbox_utm.bbox[2] + buffer_distance_meters,
        bbox_utm.bbox[3] + buffer_distance_meters
    ]
    return GeoExtent(buffered_utm, crs=utm_crs).as_geographic_bbox().bbox


def do_distanceraster(city_id):
    max_distance = MAX_DISTANCE
    amenity_name = AMENITY_NAME
    level_name = 'urbextbound'
    city_dict = {"city_id": city_id, "aoi_id": "urban_extent"}
    geo_zone = GeoZone(json.dumps(city_dict))
    city_gdf = geo_zone.zones.to_crs('EPSG:4326')
    
    amenity_points = gpd.GeoDataFrame.from_file(f'{LOCAL_AMENITYPOINTS_PREFIX}/{amenity_name}__{level_name}__{city_id}.geojson')

    if len(amenity_points) > 0:
        utm_crs = GeoExtent(amenity_points).as_utm_bbox().crs
        amenity_points_utm = amenity_points.to_crs(utm_crs)

        amenity_points['max_zone'] = amenity_points_utm.buffer(max_distance).to_crs('EPSG:4326')

    # Create geodataframe of population-pixel points by vectorizing WorldPop raster. Include only those within the boundary of interest.
    bbox = GeoExtent(city_gdf.total_bounds).as_utm_bbox().buffer_utm_bbox(500)
    utm_crs = bbox.as_utm_bbox().crs
    worldpop_data = WorldPop(agesex_classes=[]).get_data(bbox)
    wp_df = worldpop_data.drop_vars(['time']).to_dataframe().reset_index()
    pop_points = gpd.GeoDataFrame(wp_df.population, geometry=gpd.points_from_xy(wp_df.x,wp_df.y))
    pop_points_geogr = pop_points.set_crs(utm_crs).to_crs('EPSG:4326')

    # Clip to boundary
    pop_points_clipped = pop_points_geogr.loc[pop_points_geogr.intersects(city_gdf.dissolve().geometry[0])]

    b = r5py.TransportNetwork(pathlib.Path(f'{LOCAL_PBF_PREFIX}/{city_id}__{level_name}.osm.pbf'))

    if len(amenity_points) > 0:
        # Create grid, each cell of which contains spatial subset of amenity points
        amenity_grid = []
        minx, miny, maxx, maxy = float(np.min(amenity_points_utm.geometry.x)), float(np.min(amenity_points_utm.geometry.y)), float(np.max(amenity_points_utm.geometry.x)), float(np.max(amenity_points_utm.geometry.y))
        for y_idx in range(int((maxy - miny) // GRID_SIZE) + 1):
            grid_row = []
            for x_idx in range(int((maxx - minx) // GRID_SIZE) + 1):
                xmin, xmax = minx + (x_idx * GRID_SIZE), (minx) + ((x_idx + 1) * GRID_SIZE)
                ymin, ymax = miny + (y_idx * GRID_SIZE), (miny) + ((y_idx + 1) * GRID_SIZE)
                amenity_points_gridcell = amenity_points_utm.loc[amenity_points_utm.within(shapely.box(xmin, ymin, xmax, ymax))]
                grid_row.append(list(amenity_points_gridcell.id))
            amenity_grid.append(grid_row)

        def traveldist_onecell(amenity_ids):
            max_zones = amenity_points.loc[amenity_points.id.isin(amenity_ids)].max_zone.union_all()
            pop_points_withinmax = pop_points_clipped.loc[pop_points_clipped.intersects(max_zones)]
            d = r5py.TravelTimeMatrix(
                transport_network=b,
                origins=gpd.GeoDataFrame({'id': amenity_ids, 'geometry': amenity_points.loc[amenity_points.id.isin(amenity_ids)].to_crs(utm_crs).centroid.to_crs('EPSG:4326')}),
                destinations=gpd.GeoDataFrame({'id': pop_points_withinmax.index, 'geometry': pop_points_withinmax.geometry}),
                transport_modes=[TRAVEL_MODES['walk']],
                max_time_walking=datetime.timedelta(minutes=500),
                speed_walking=0.6
            )
            traveldist = d.pivot(index='to_id', columns='from_id', values='travel_time').min(axis=1, skipna=True) * 10
            return traveldist

        res = pd.DataFrame({'to_id': pop_points_clipped.index, 'traveldist': [10000] * len(pop_points_clipped)}).set_index('to_id')
        for row in amenity_grid:
            for col in row:
                if col:
                    traveldist = traveldist_onecell(col)
                    res['newvals'] = res.traveldist
                    res.loc[traveldist.index, 'newvals'] = traveldist
                    res.traveldist = res[['traveldist', 'newvals']].min(axis=1)
                    res = res.drop('newvals', axis=1)
        res.loc[res.traveldist==10000, 'traveldist'] = pd.NA
        

        # Take result dataframe and merge results back into the population-pixel geodataframe
        pop_points['traveldist'] = 0
        if len(amenity_points) > 0:
            pop_points.loc[pop_points_clipped.index, 'traveldist'] = res['traveldist']
            # pop_points.traveldist = pop_points.traveldist.fillna(0).astype(int)

        # Convert pop-pixel gdf with number-of-accessible-points data to raster
        geo_grid = make_geocube(
            vector_data=pop_points.set_crs(utm_crs),
            measurements=['traveldist'],
            like = worldpop_data,
            rasterize_function=rasterize_points_griddata,
        )
        return geo_grid.traveldist
    else:
        return None


def do_city(city_id):
    print(f"Starting {city_id}")
    level_name = 'urbextbound'
    res = do_distanceraster(city_id)
    if res is not None:
        ofilename = f"{AMENITY_NAME}__{city_id}__{level_name}__distance_to_nearest__meters.tif"
        res.rio.to_raster(f"{LOCAL_DISTANCERASTER_PREFIX}/{ofilename}")
        upload_s3(SESSION, f"{LOCAL_DISTANCERASTER_PREFIX}/{ofilename}", BUCKET, f"{S3_DISTANCERASTER_PREFIX}/{ofilename}")
        print(f"**** {city['name']} done ****")
        return True
    else:
        return False
        

for city in focal_cities:
    do_city(city['id'])
