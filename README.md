# Build Covid Database

This code is used to compile covid data from each county and save it to a local drive as .csv files, as well as to a MySQL server. Data includes positive cases, confirmed deaths, google trend data per state, and various statistics like population density, death rate, etc. 

## How the Code Works:

```
local_directory = 
```
The Local Directory where csv files are saved is at the beginning and can be edited. 

The program looks in that directory for an .ini file, if one is not found, one is created as covid19_config.ini




