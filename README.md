# Covid Database Builder

### Requirements
- MySQL server setup through XAMPP

This code is used to compile covid data from each county in every state and save it to a local drive as .csv files, as well as to a MySQL server. Data includes positive cases, confirmed deaths, google trend data per state, and various statistics like population density, death rate, etc. 

## How the Code Works:

```
local_directory = 
```
- The Local Directory where csv files are saved is at the beginning and can be edited, the default is C:/
------------------
```
self.keywords = ['covid']
```
- You can customize which keywords are included in the google trend search. Right now it only includes "covid". 
------------------
```
def _create_database(directory):
    _database_directory = Path(directory) / f'COVID19/'
    if not os.path.isdir(_database_directory):
        _database_directory = f'C:/COVID19/'
        os.makedirs(_database_directory)
        return Path(_database_directory)
    return Path(_database_directory)
```
- This function is used to create the locally stored database, where all data will be saved. 
------------------
```
def _find_config_file(self):
    config_file = self.database_directory / 'covid19_config.ini'
    if not os.path.isfile(config_file):
        with open(config_file, 'a') as cfg:
            self.write_config.write(cfg)
        config_file = self.database_directory / 'covid19_config.ini'
    return config_file
```
- This function looks for an .ini present in the directory, if one is not found, one will be created using the __write_ini_params()_ function. 
------------------
```
def _database_running_check(self):
    call = 'TASKLIST', '/FI', 'imagename eq %s' % 'xampp-control.exe'

    # use buildin check_output right away
    output = subprocess.check_output(call).decode()

    # check in last line for process name
    last_line = output.strip().split('\r\n')[-1]

    # because Fail message could be translated
    if last_line.lower().startswith('xampp-control.exe'.lower()):
        pass
    else:
        # Start MySQL Database #
        self._printout('Database not Running, Restarting...')
        # subprocess.run('"D:/xampp/xampp-control.exe"')
        subprocess.Popen(["D:/xampp/xampp-control.exe"], shell=True)
        sleep(2)

        # Restart #
        self.run()
```
- This function checks if the MySQL server is up and running, if not, XAMPP.exe is executed which should start the database. 
------------------
```
def _mysql(self, db):
    self.read_config.read(self.config)
    self.db = db

    root = self.read_config.get("mysql", "user")
    host = self.read_config.get("mysql", "host")
    db = self.db

    # MySQL Connection #
    my_conn = create_engine(
        f"mysql+mysqlconnector://{root}:@{host}/{db}",
        connect_args={'connect_timeout': 600})
    if not database_exists(my_conn.url):
        create_database(my_conn.url)
    return my_conn
```
- MySQL Handler
------------------
```
  def _population_data(self):
```
- This function pulls the most recent census data for each county, as well as the land_area of each county. This information is cleaned and reformatted to calculate population density. 
------------------
```
def _google_trends(self):
```
- This function compiles the relative google search trend values for the keyword "covid", for each state, then saves that all to the database. 
------------------
```
def _get_historical_data():
def _get_live_data():
def _merge_data(self):
def _clean_data(self):
```
- These four functions are self-explanatory. Historical data is pulled along with the most recent data from the last 24 hours. This is merged into one table then sorted and cleaned. Duplicates are removed and unknown values are removed. Then the data is used to calculate cases/deaths per 1k people, along with 7-day moving averages. 
------------------
```
def run():
    with Covid_Database(local_directory):
        sleep(43200)
        run()
```
- All of this is executed via the _run()_ function, which provides text output for each step of the process. The sleep timer is set so the data is updated every 12 hours, or 43200 seconds. 
------------------



