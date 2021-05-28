# Pedestrian Count
Public Pedestrian Count in the Melbourne CBD

## Source Data - Pedestrian Counting System
https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-2009-to-Present-counts-/b2ak-trbp
https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Sensor-Locations/h57g-5234 

## Features
The features that were implemented are 
- Parse through data (either directly from the web or locally if data exists)
- Combine information from sensor location and pedestrian count tables into a single dataframe
- Accumulate daily / monthly pedestrian totals
- Store top 10 locations per month / day in a database

## Running the program
To run the program sucessfully you need to setup the appropriate environment using the command below. Note: "env.yml" file is provided in this repo.
```
conda env create -f env.yml
```

Once the environment has been setup run the following command to activation the virtual environment
```
activate belong
```

Once everything is set up the program by typing
```
python Pedestrians.py
```
If you already have the pedestrian data and sensor location data on you machine locally you can use the following command
```
python Pedestrians.py -l
```
If you already have the data you want to upload to s3 ready then run the file with "-csv" option
```
python Pedestrians.py -csv
```
Note that for this option you will have to enter the file names of the corresponding data files


