# Pedestrian Count
Public Pedestrian Count in the Melbourne CBD

## Source Data - Pedestrian Counting System
https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-2009-to-Present-counts-/b2ak-trbp
https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Sensor-Locations/h57g-5234 

## Features
The features that were implemented are 
- Parse through data (either directly from the web or locally if data exists)
- Accumulate daily / monthly pedestrian totals
- Combine information from sensor location and pedestrian count tables into a single dataframe
- Store top 10 locations per month / day in a database
- 
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
python pedestrians.py
```
