# Generating amenity distancerasters
There are two scripts here. They are intended to be downloaded, modified, and run on your local computer.
* [get_amenitypoints_for_distancerasters.py](https://github.com/wri/cities-cif-preprocessing/blob/main/amenity_distancerasters/get_amenitypoints_for_distanceraster.py) queries OpenStreetMap for points associated with the specified amenities, and stores a geodataframe (as geojson) to your local computer.
* [get_store_distancerasters.py](https://github.com/wri/cities-cif-preprocessing/blob/main/amenity_distancerasters/get_store_distancerasters.py) reads the locally stored amenity points GDF and stores (locally and in an S3 bucket) a distance raster.

Both scripts require [CIF](https://github.com/wri/cities-cif) and its associated dependencies--including Google and AWS credentials. Additionally, get_store_distancerasters.py requires...
* [r5py](https://r5py.readthedocs.io/stable/), which in turn requires Java Development Kit v21+. The installation process is tricky on Windows.
* Locally stored PBF files (many are stored [here](s3://wri-cities-indicators/devdata/inputdata/pbf/)) of your cities' street networks.

Note that both scripts must be modified before they can be run. You must specify amenity types, cities, and local and S3 paths.

Direct questions to Ted Wong (ted.wong@wri.org).
