# pedestrian-count
Public Pedestrian Count in the Melbourne CBD

## Source Data - Pedestrian Counting System
https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-2009-to-Present-counts-/b2ak-trbp
https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Sensor-Locations/h57g-5234 

## Feature
The features that were implemented are 
- Parse through 
- Top 10 (most pedestrians) locations by day
- Top 10 (most pedestrians) locations by month## Load- Data into S3 in
an appropriate format for future querying
Statistical data into an appropriate data store.

This repository contains a program that accesses the data from 
https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Monthly-counts-per-hour/b2ak-trbp and
https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Sensor-Locations/h57g-5234

The aim of the program is to find (and visualise)
Top 10 locations on a daily basis and 
Top 10 locations on a monthly basis

To run the program you need to setup the environment using the following command (using the "env.yml" file in this project)
```
conda env create -f env.yml
```

Once the environment has been setup run the following command
```
activate belong
```

Finally to run the program type 
```
python pedestrians.py
```
