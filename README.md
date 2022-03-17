# Covid Database Builder for Visualization in Tableau


This code is used to compile covid data from each county, in every state, then saved to C:\COVID19\ as .csv files, and uploaded to MySQL server if you want. Data includes positive cases, confirmed deaths, google search trend data per state, vaccination rates, and various statistics like population density, death rate, etc. 

## Tableau Dashboard using this Data: 
### https://public.tableau.com/app/profile/jordan.bradley/viz/CovidTracking_16443875223840/Dashboard
------------------
# How the Code Works:

```
local_directory = Path(f'C:/COVID19/')
```
- The Local Directory where csv files are saved is at the beginning and can be edited, the default is C:/
------------------
```
class config_handler:
```
- This class creates and reads a configuration file that contains the MySQL server information, database location, etc.
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
        # Configuration Variables #
        self.config = local_directory / 'covid19_config.ini'
        self.write_config = configparser.ConfigParser(strict=False)
        self.read_config = configparser.ConfigParser(strict=False)

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
- MySQL connection handler
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
def _vaccine_data(self):
```
- This function gathers the number of administered vaccines for each state. 
```
def _get_historical_data():
def _get_live_data():
def _merge_data(self):
def _clean_data(self):
```
- These four functions are self-explanatory. Historical data is pulled along with the most recent data from the last 24 hours. This is merged into one table then sorted and cleaned. Duplicates are removed and unknown values are removed. Then the data is used to calculate cases/deaths per 1k people, along with 14-day moving averages. 
------------------
```
Covid_Database().run()
```
- All of this is executed via the _run()_ function, which provides text output for each step of the process.
------------------

### To-Do:
- Compile to .exe

