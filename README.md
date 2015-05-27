# GeoNamesIngest
A python tool that ingests geo data from the National Geospatial Agency's geo names repository into a local sqlite database for fast searching

## Prequisites
* python
* sqlite
* latest NGA Geo Names text dump available [here](http://geonames.nga.mil/gns/html/namefiles.html)

## Usage

After downloading and extracting the latest geo names dump, run the python script

```bash
./ingestGeoNamesData.py <geo_names_dump.txt>
```

The process can take some time, and upon completion a geonames.db file is created with a subset of all geo names data.

