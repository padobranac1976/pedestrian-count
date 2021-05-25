# pedestrian-count
Public Pedestrian Count in the Melbourne CBD

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
