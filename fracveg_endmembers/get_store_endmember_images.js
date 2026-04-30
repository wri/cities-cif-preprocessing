// This Google Earth Engine script stores an ImageCollection, each Image of which is a box
// with two bands--one for each of the endmembers required to calculate the FractionalVegetation
// layer in CIF.

var PCTL_FULLVEG = 75
var PCTL_NONVEG = 5
var dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
var S2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
var S2CS = ee.ImageCollection("GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED")
var CLEAR_THRESHOLD = 0.60
var YEAR = 2025

var TARGET_CITIES = [
 'ARG-Buenos_Aires',
 'BRA-Campinas',
 'BRA-Florianopolis',
 'BRA-Fortaleza',
 'BRA-Recife',
 'BRA-Rio_de_Janeiro',
 'BRA-Teresina',
 'IDN-Jakarta',
 'IND-Bhopal',
 'KEN-Nairobi',
 'MEX-Hermosillo',
 'MEX-Mexico_City',
 'MEX-Monterrey',
 'USA-Boston',
 'ZAF-Cape_Town',
 'ZAF-Durban',
 'ZAF-Johannesburg'
];

var BBOX_FC = ee.FeatureCollection("projects/wri-datalab/cities/bboxes/cif_bboxes");

for(var idx=0; idx<TARGET_CITIES.length; idx++){
  var city_id = TARGET_CITIES[idx];
  var bbox = BBOX_FC.filter(ee.Filter.eq('city_id', city_id)).first().geometry();
  var date_start = YEAR.toString() + '-01-01';
  var date_end = (YEAR + 1).toString() + '-01-01';

  var dwFiltered = dw.filterBounds(bbox).filterDate(date_start, date_end);
  var dwMode = dwFiltered.select("label").reduce(ee.Reducer.mode()).clip(bbox);
  
  var dwClassVeg = (
      dwMode.remap([0, 1, 2, 3, 4, 5, 6, 7, 8], [0, 1, 1, 0, 0, 1, 0, 0, 0])
      .selfMask()
      .rename("lc")
  );
  var dwClassSoil = (
      dwMode.remap([0, 1, 2, 3, 4, 5, 6, 7, 8], [0, 0, 0, 0, 0, 0, 2, 0, 0])
      .selfMask()
      .rename("lc")
  );
  
  var samplepointsVeg = dwClassVeg.stratifiedSample({numPoints:1000, scale: 10, geometries: true, dropNulls: true});
  var samplepointsSoil = dwClassSoil.stratifiedSample({numPoints:1000, scale: 10, geometries: true, dropNulls: true});
  
  var vegNDVI = ee.List([]);
  var soilNDVI = ee.List([]);
  
  function clipImg(img) {
    return img.clip(bbox);
  }
  function processImg(img) {
    var result = img.updateMask(
      img.select("cs").gte(CLEAR_THRESHOLD))
        .select(['B4', 'B8'])
        .normalizedDifference(["B8", "B4"])
        .rename("NDVI")
        .select('NDVI')
        .multiply(1000)
        .round();
    return result
  }
  
  function getSampleVeg(img) {
    return img.sampleRegions({collection: samplepointsVeg, scale: 10, properties: []})
  }
  function getSampleSoil(img) {
    return img.sampleRegions({collection: samplepointsSoil, scale: 10, properties: []})
  }
  

  var S2filtered = (
      S2.filterBounds(bbox)
      .filterDate(YEAR.toString() + '-01-01', (YEAR + 1).toString() + '-01-01')
      .map(clipImg)
      .linkCollection(S2CS.map(clipImg), ["cs"])
      .map(processImg)
  );
  var pointsVeg = S2filtered.map(getSampleVeg).flatten();
  var pointsSoil = S2filtered.map(getSampleSoil).flatten();
  vegNDVI = vegNDVI.add(pointsVeg.aggregate_array('NDVI')).flatten();
  soilNDVI = soilNDVI.add(pointsSoil.aggregate_array('NDVI')).flatten();
  
  var fullveg_ndvi = vegNDVI.reduce(ee.Reducer.percentile([PCTL_FULLVEG]));
  var nonveg_ndvi = vegNDVI.reduce(ee.Reducer.percentile([PCTL_NONVEG]));
  
  var fullveg_img = ee.Image(ee.Number(fullveg_ndvi).divide(1000)).clip(bbox).rename("fullveg_ndvi");
  var nonveg_img = ee.Image(ee.Number(nonveg_ndvi).divide(1000)).clip(bbox).rename("nonveg_ndvi");
  var output_box = fullveg_img.addBands(nonveg_img);
  
  Export.image.toAsset({
        image: output_box,
        description: 'byyear_' + city_id,
        assetId: 'projects/wri-datalab/cities/heat/fracveg_endmembers_bybbox/' + city_id + '__' + YEAR.toString(),
        scale: 10,
        crs: 'EPSG:4326',
        maxPixels: 1e13
    });
  print("Done " + city_id);
}
