import geopandas as gp

data = gp.read_file("block_group.geojson")
data.to_parquet("sample.parq")
