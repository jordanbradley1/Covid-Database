# Build Covid Database

### Requirements
- MySQL server setup through XAMPP
- Excel


This code is used to compile covid data from each county and save it to a local drive as .csv files, as well as to a MySQL server. Data includes positive cases, confirmed deaths, google trend data per state, and various statistics like population density, death rate, etc. 

## How the Code Works:

```
local_directory = 
```
The Local Directory where csv files are saved is at the beginning and can be edited. 

All processes are wrapped in the Covid_Database class.
The program looks in that directory for an .ini file, if one is not found, one is created as covid19_config.ini. This .ini file saves the database location, and MySQL databse names for the database, google trend data, and population data. 

```
_database_running_check():
```
This function checks if the MySQL server is up and running, if not, XAMPP.exe is executed which should start the database. 

```
_population_data():
```
This function pulls the most recent census data for each county, as well as the land_area of each county. This information is cleaned and reformatted to calculate population density per state. 

```
_google_trends():
```
This function compiles the relative google search trend for each state for the term "covid". 


To-Do
Explain 
> _get_historical_data
> _get_live_data
> _clean_data
> _run





