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

The program looks in that directory for an .ini file, if one is not found, one is created as covid19_config.ini. This .ini file saves the database location, and MySQL databse names for the database, google trend data, and population data. 

```
_database_running_check():
```
This function checks if the MySQL server is up and running, if not, XAMPP.exe is executed which should start the database. 




