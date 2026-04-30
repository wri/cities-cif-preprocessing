import os, json, requests, boto3, sys
import geopandas as gpd
import pandas as pd
import osmnx as ox
from city_metrix.metrix_model import GeoExtent, GeoZone
from city_metrix.metrix_tools import get_utm_zone_from_latlon_point
from city_metrix.layers import OpenStreetMapClass

'''
This script gets amenity points from OSM and saves them to a local file, which can be read by
the script get_store_distancerasters.py. Specify amenity list, local filepath, project name, and cities.
'''

# CHANGE THESE THREE LINES
LOCAL_AMENITYPOINTS_PREFIX = 'C:/Users/tgwon/wri/indicators/demodata/amenitypoints'
TARGET_AMENITYTYPES = [OpenStreetMapClass.SUBWAY_STATION]
PROJECT_NAME = 'subways'


citydata_url = 'https://dev.cities-data-api.wri.org/cities'
citydata = requests.get(citydata_url).json()
# CHANGE THIS TO SELECT PARTICULAR CITIES
focal_cities = [c for c in citydata['cities'] if c['id']=='CHN-Chengdu']#'dataforcoolcities' in c['projects']]
# focal_cities = [c for c in citydata['cities'] if 'dataforcoolcities' in c['projects']]

SAMPLE_SEPARATION = 50 # meters


def get_data_from_polygon(polygon, osm_class):
    # Set the OSMnx configuration to disable caching
    ox.settings.use_cache = False
    try:
        #osm_feature = ox.features_from_bbox(bbox=(min_lon, min_lat, max_lon, max_lat), tags=self.osm_class.value)
        osm_feature = ox.features_from_polygon(polygon, tags=osm_class.value)
    # When no feature in bbox, return an empty gdf
    except ox._errors.InsufficientResponseError as e:
        osm_feature = gpd.GeoDataFrame(pd.DataFrame(columns=['id', 'geometry']+list(osm_class.value.keys())), geometry='geometry')
        osm_feature.crs = "EPSG:4326"

    # Filter by geo_type
    if osm_class == OpenStreetMapClass.ROAD:
        # Filter out Point
        osm_feature = osm_feature[osm_feature.geom_type != 'Point']
    elif osm_class == OpenStreetMapClass.TRANSIT_STOP:
        # Keep Point
        osm_feature = osm_feature[osm_feature.geom_type == 'Point']
    else:
        # Filter out Point and LineString
        osm_feature = osm_feature[osm_feature.geom_type.isin(['Polygon', 'MultiPolygon'])]

    # keep only columns desired to reduce file size
    keep_col = ['id', 'geometry']
    for key in osm_class.value:
        if key in osm_feature.columns:
            keep_col.append(key)
    # keep 'lanes' for 'highway'
    if 'highway' in keep_col and 'lanes' in osm_feature.columns:
        keep_col.append('lanes')
    osm_feature = osm_feature.reset_index()[keep_col]

    return osm_feature

def get_perimeter_points(city_gdf, osm_class):
    utm_crs = get_utm_zone_from_latlon_point(city_gdf.dissolve().geometry[0].centroid)
    openspace_polys = get_data_from_polygon(city_gdf.dissolve().geometry[0], osm_class).explode().reset_index(drop=True)
    perimeter_points = []
    for rownum in range(len(openspace_polys)):
        geom = openspace_polys.iloc[[rownum]].to_crs(utm_crs).geometry[rownum]
        perim = geom.exterior
        perimeter_points += [perim.interpolate(i * SAMPLE_SEPARATION) for i in range(int(perim.length / SAMPLE_SEPARATION))]
    return gpd.GeoDataFrame({'id': range(len(perimeter_points)), 'geometry': perimeter_points}, crs=utm_crs).to_crs('EPSG:4326')

def merge_osm_classes(osm_classes):
    result = {}
    for d in osm_classes:
        d = d.value
        for k in d:
            if not (k in list(result.keys())):
                result[k] = []
            if result[k] == True or d[k] == True:
                result[k] = True
            else:
                result[k] += d[k]
    return result

def get_amenities_pointsonly(city_gdf, osm_classes):
    bbox = GeoExtent(city_gdf.total_bounds)
    utm_epsg = bbox.as_utm_bbox().epsg_code
    polygon = city_gdf.dissolve().geometry[0]
    merged_osm_dicts = merge_osm_classes(osm_classes)
    # Set the OSMnx configuration to disable caching
    ox.settings.use_cache = False
    try:
        #osm_feature = ox.features_from_bbox(bbox=(min_lon, min_lat, max_lon, max_lat), tags=self.osm_class.value)
        osm_feature = ox.features_from_polygon(polygon, tags=merged_osm_dicts)
    # When no feature in bbox, return an empty gdf
    except ox._errors.InsufficientResponseError as e:
        osm_feature = gpd.GeoDataFrame(pd.DataFrame(columns=['osmid', 'geometry']+list(merged_osm_dicts.keys())), geometry='geometry')
        osm_feature.crs = "EPSG:4326"

    osm_feature = osm_feature[osm_feature.geom_type.isin(['Point', 'Polygon', 'MultiPolygon'])].reset_index().rename(mapper={'id': 'osmid'}, axis=1)
    # keep only columns desired to reduce file size
    keep_col = ['osmid', 'geometry'] + list(merged_osm_dicts.keys())
    for col in keep_col:
        if not col in osm_feature.columns:
            osm_feature[col] = [pd.NA] * len(osm_feature)

    osm_feature = osm_feature[keep_col]
    osm_feature.geometry = osm_feature.to_crs(utm_epsg).centroid.to_crs('epsg:4326')
    
    result = {}
    for osm_class in osm_classes:
        class_name = osm_class.__str__().lower().split('.')[1].replace('_', '-')
        osm_dict = osm_class.value
        to_keep = ['geometry'] + list(osm_dict.keys())
        result[class_name] = gpd.GeoDataFrame(columns=to_keep, geometry='geometry').set_crs('EPSG:4326')
        for k in osm_dict:
            if osm_dict[k] == True:
                to_append = osm_feature.loc[osm_feature[k].notnull()][to_keep]
            else:
                to_append = osm_feature.loc[osm_feature[k].isin(osm_dict[k])][to_keep]
            result[class_name] = pd.concat([result[class_name], to_append])
    return result


for city in focal_cities:
    if True:
        print(city['id'])
        city_dict = {"city_id": city['id'], "aoi_id": "urban_extent"}
        geo_zone = GeoZone(json.dumps(city_dict))
        city_urbext = geo_zone.zones.to_crs('EPSG:4326')
        boundary = ('urbext', city_urbext)
        boundaryname = boundary[0]
        boundary_gdf = boundary[1]
        ofilename = f"{PROJECT_NAME}__{boundaryname}bound__{city['id']}.geojson"

        all_results = []
        for amenity in TARGET_AMENITYTYPES:
            print(amenity.name)
            if not amenity in [OpenStreetMapClass.OPEN_SPACE, OpenStreetMapClass.OPEN_SPACE_HEAT]:
                amenity_result = list(get_amenities_pointsonly(boundary_gdf, [amenity]).values())[0]
                amenity_result['amenity_class'] = amenity.name
                amenity_result = amenity_result[['amenity_class', 'geometry']].reset_index()
            else:
                success = False
                while not success:
                    try:
                        amenity_result = get_perimeter_points(boundary_gdf, amenity)
                        success = True
                    except:
                        pass
            all_results.append(amenity_result)
        amenity_results = pd.concat(all_results)
        
        filtered_results = amenity_results.loc[amenity_results.within(boundary_gdf.dissolve().geometry[0])].reset_index()
        with open(f"{LOCAL_AMENITYPOINTS_PREFIX}/{ofilename}", 'w') as ofile:
            ofile.write(filtered_results.to_json())
        print()
