# Generate and store citywide endmembers for fractional vegetation calculations

The two scripts here generate citywide NDVI values for full vegetation and bare soil, referred to as _endmembers_ in the calculation of fractional vegetation in CIF.
Calculating citywide values is necessary because layers for large cities are freuently calculated as tiles, and it is important that each city's separate tile calculations draw on a single set of endmembers.
* The script _save_cif_boxes.py_ stores bboxes in GEE for each of the target cities.
* The script _get_store_endmember_images.js_ is meant to be copied and pasted into the GEE Code Editor, modified, and then run. It finds the endmember values for each city and stores them in the bands of images in a GEE ImageCollection.
  
