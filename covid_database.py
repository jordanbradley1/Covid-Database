#!/usr/bin/env python

import os
import datetime
from time import sleep
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from threading import Thread
from shutil import get_terminal_size
import configparser
import subprocess
from pytrends.request import TrendReq

# Pandas Option to set Infinity values as NaN
pd.set_option('use_inf_as_na', True)

##########################################################
# Enter Local Database Directory Here | Default is C:/ ###
local_directory = f'I:/My Drive/'


##########################################################


# Build Covid Database per State #
class Covid_Database(object):
    """ Pull New York Times Covid Case/Death Data per State/County and Store in MySQL """

    def __init__(self, database_directory):
        # Now Datetime #
        self.now = f'{datetime.datetime.now():%Y-%m-%d %H:%M:%S}'

        # Lead Text of Printout #
        self.lead_text = 'Current Operation: '

        # Location of Main Database CSV backups #
        self.database_directory = Path(database_directory + '/COVID19/')

        # Configuration Variables #
        self.config_name = self.database_directory / 'covid19_config.ini'
        self.write_config = configparser.ConfigParser(strict=False)
        self.read_config = configparser.ConfigParser(strict=False)
        self.config = self._find_config_file()

        # Blank Dataframe used to store data #
        self.df = pd.DataFrame()
        self.dict = {}
        self._population_dict = {}

        # State Abbreviation Dictionary #
        self.states = {
            'AK': 'Alaska',
            'AL': 'Alabama',
            'AR': 'Arkansas',
            'AZ': 'Arizona',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DC': 'District of Columbia',
            'DE': 'Delaware',
            'FL': 'Florida',
            'GA': 'Georgia',
            'HI': 'Hawaii',
            'IA': 'Iowa',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'MA': 'Massachusetts',
            'MD': 'Maryland',
            'ME': 'Maine',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MO': 'Missouri',
            'MS': 'Mississippi',
            'MT': 'Montana',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'NE': 'Nebraska',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NV': 'Nevada',
            'NY': 'New York',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VA': 'Virginia',
            'VT': 'Vermont',
            'WA': 'Washington',
            'WI': 'Wisconsin',
            'WV': 'West Virginia',
            'WY': 'Wyoming'
        }

        # Google Keywords #
        self.keywords = ['covid']

        # Handling multithreading #
        self.thread_ = Thread(target=self.run, daemon=True)
        self.done = False

    # Thread Starter #
    def _thread_start(self):
        self.thread_.start()
        return self

    # Thread Stopper #
    def _thread_stop(self):
        self.done = True
        cols = get_terminal_size((80, 20)).columns
        self._printout(f"\rCompleted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}" + " " * cols)

    @staticmethod
    def _create_database(directory):
        _database_directory = Path(directory) / f'COVID19/'
        if not os.path.isdir(_database_directory):
            _database_directory = f'C:/COVID19/'
            os.makedirs(_database_directory)
            return Path(_database_directory)
        return Path(_database_directory)

    # Function to simplify the printouts #
    def _printout(self, text):
        print(f'\r{self.lead_text} {text}', flush=True, end="")
        sleep(1)

    # Locate Configuration File #
    def _find_config_file(self):
        config_file = self.database_directory / 'covid19_config.ini'
        if not os.path.isfile(config_file):
            with open(config_file, 'a') as cfg:
                self.write_config.write(cfg)
            config_file = self.database_directory / 'covid19_config.ini'
        return config_file

    # Write Parameters to Configuration File #
    def _write_ini_params(self):
        # initial ini read #
        self.read_config.read(self.config)

        # write database location #
        for section in ['database']:
            if not self.read_config.has_section(section):
                self.write_config.add_section('database')

                for option in ['location']:
                    if not self.read_config.has_option(section, option):
                        self.write_config[str(section)][str(option)] = str(self.database_directory)

        # write mysql settings #
        for section in ['mysql']:
            if not self.read_config.has_section(section):
                self.write_config.add_section('mysql')

                for option in ['host', 'user']:
                    if not self.read_config.has_option(section, option):
                        self.write_config[str(section)]['host'] = 'localhost'
                        self.write_config[str(section)]['user'] = 'root'

        # write google trend settings #
        for section in ['google']:
            if not self.read_config.has_section(section):
                self.write_config.add_section('google')

                for option in ['db']:
                    if not self.read_config.has_option(section, option):
                        self.write_config[str(section)]['db'] = 'google_trend'

        # write covid database settings #
        for section in ['covid']:
            if not self.read_config.has_section(section):
                self.write_config.add_section('covid')

                for option in ['db']:
                    if not self.read_config.has_option(section, option):
                        self.write_config[str(section)]['db'] = 'covid'

        # write population settings #
        for section in ['population']:
            if not self.read_config.has_section(section):
                self.write_config.add_section('population')

                for option in ['db']:
                    if not self.read_config.has_option(section, option):
                        self.write_config[str(section)]['db'] = 'population'

        with open(self.config, 'a') as f:
            self.write_config.write(f)

    # Check if Database is Running #
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

    # Setup MySQL Connection #
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
        return my_conn

    # Get Population per FIPS Code #
    def _population_data(self):
        """
        Pulls Population Data per FIPS code from GitHub
        :rtype: Dataframe Object, CSV File
        """

        # Pull Census Population Data #
        data = f'https://www.ers.usda.gov/webdocs/DataFiles/48747/PopulationEstimates.csv?v=3278.6'

        # Clean Data #
        data = pd.read_csv(data, usecols=['FIPStxt', 'State', 'Area name', 'Attribute', 'Value'])
        data = data.loc[data['Attribute'] == 'Population 2020'].reset_index(drop=True).drop(columns=['Attribute'])

        # Rename Columns #
        data = data.rename(
            columns={'FIPStxt': 'fips', 'State': 'state', 'Area name': 'location', 'Value': 'population'}
        )

        # Land Area Data #
        land_area = pd.read_excel(
            'https://www2.census.gov/library/publications/2011/compendia/usa-counties/excel/LND01.xls',
            usecols=['STCOU', 'LND010200D']
        )

        # Rename Columns #
        land_area = land_area.rename(columns={'STCOU': 'fips', 'LND010200D': 'land_area'})

        # Merge Population and Land Area #
        merged_data = data.merge(land_area, how='inner', on=['fips'])

        # Calculate population density #
        merged_data['population_density'] = round(merged_data['population'] / merged_data['land_area'], 2)
        merged_data['state'] = merged_data['state'].replace(self.states)
        merged_data = merged_data.fillna(0)

        # Format Data #
        merged_data['fips'] = merged_data['fips'].astype(str)
        merged_data['fips'] = merged_data['fips'].str.zfill(5)

        merged_data.to_sql(
            name='population_data',
            con=self._mysql('population'),
            if_exists='replace',
            index=False,
            chunksize=100,
            method='multi'
        )

        merged_data.to_csv(self.database_directory / 'population_data.csv')

    # Create Population Dictionary #
    def _create_population_dict(self):
        _data = pd.read_csv(f'{self.database_directory}/population_data.csv', index_col='fips')
        _data.index = _data.index.astype(str).str.zfill(5)
        return _data['population'].to_dict()

    def _google_trends(self):
        def _get_searches(_state, _keywords):
            """
            Function to get Google Trend Data by Keyword per state
            :param _state:
            """
            pytrends = TrendReq(hl='en-US', tz=360)

            if _state == 'US':
                _geo = 'US'
            else:
                _geo = f'US-{_state}'

            pytrends.build_payload(
                _keywords,
                cat=0,
                timeframe=f'2020-01-01 {datetime.datetime.today():%Y-%m-%d}',
                gprop='',
                geo=_geo
            )
            ######

            # Return Dataframe #
            df = pytrends.interest_over_time()
            df['timestamp'] = pd.to_datetime(df.index, format='%Y-%m-%d')
            ######
            return df

        # set dummy index #
        self.df.index = _get_searches('US', self.keywords).index

        # Get Data per State #
        for s in self.states.keys():
            google_data = _get_searches(s, self.keywords)[self.keywords]
            self.df[self.states[s].upper()] = google_data

        # Clean and Reformat Data #
        self.df = self.df.stack().reset_index(drop=False).rename(columns={'level_1': 'state', 0: 'google_trend'})

        # Save to CSV #
        self.df.to_csv(self.database_directory / 'google_trend_data.csv', index=True)

        # Add to MySQL database #
        self.df.to_sql(
            name='google_trends',
            con=self._mysql('google_trend'),
            if_exists='replace',
            index=False,
            chunksize=100,
            method='multi',
        )

    # Get Historical Data #
    @staticmethod
    def _get_historical_data():
        # Historical Data Variable #
        historical_url = f'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'

        # Create DataFrame from Data #
        historical_data = pd.read_csv(historical_url, dtype=object)

        return historical_data

    # Get Live Data #
    @staticmethod
    def _get_live_data():
        # Create DataFrame object from live data #
        live_data = pd.read_csv(
            f'https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/us-counties.csv',
            usecols=[0, 1, 2, 3, 4, 5], dtype=object)

        return live_data

    def _merge_data(self):
        _historical_data = self._get_historical_data()
        _live_data = self._get_live_data()

        # Merge Historical and Live Data #
        self._printout('Merging Data')
        _data = pd.concat([_live_data, _historical_data])

        return _data

    def _clean_data(self):
        # Create Case Data Folder if Needed #
        _state_data_directory = f'{self.database_directory}/state_data/'
        if not os.path.isdir(_state_data_directory):
            os.mkdir(_state_data_directory)

        # Format State and County names to Uppercase #
        _data = self._merge_data()
        _data['state'] = _data['state'].str.upper()
        _data['county'] = _data['county'].str.upper()

        # Delete Duplicates and Sort #
        self._printout('Removing Duplicates and Sorting')
        _data = _data.drop_duplicates(ignore_index=True)
        _data = _data.sort_values(by=['state', 'county', 'date'])
        _data = _data.reset_index(drop=True)

        # Remove Unknown Fips Values #
        _data = _data.loc[_data['state'] != 'Guam'.upper()]
        _data = _data.loc[_data['state'] != 'Northern Mariana Islands'.upper()]
        _data = _data.loc[_data['state'] != 'Virgin Islands'.upper()]
        _data = _data.loc[_data['state'] != 'American Samoa'.upper()]
        _data = _data.loc[_data['state'] != 'Puerto Rico'.upper()]
        _data = _data.loc[_data['county'] != 'Unknown'.upper()]

        # Format Data for Extra Calculations #
        self._printout('Data Conversion')
        _data['cases'] = _data['cases'].fillna(0).astype('int32')
        _data['deaths'] = _data['deaths'].fillna(0).astype('int32')
        _data['date'] = pd.to_datetime(_data['date'])

        # Calculate Daily Cases/Deaths and other various calculations #
        self._printout('Additional Calculations')
        _data["cases_daily"] = _data.groupby('fips')["cases"].diff(1)
        _data["deaths_daily"] = _data.groupby('fips')["deaths"].diff(1)
        _data["cases_daily"] = _data["cases_daily"].fillna(0).astype('int32')
        _data["deaths_daily"] = _data["deaths_daily"].fillna(0).astype('int32')

        # Infected Death Rate #
        _data['death_rate'] = (_data['deaths'] / _data['cases'])
        _data['death_rate'] = _data['death_rate'].round(4)

        # Per 1k #
        _data['fips'] = _data['fips'].astype(str).str.zfill(5)
        _data = _data.loc[_data['fips'] != '00nan']
        _data = _data.loc[_data['fips'] != '02997']
        _data = _data.loc[_data['fips'] != '02158']
        _data = _data.loc[_data['fips'] != '02261']
        _data = _data.loc[_data['fips'] != '02998']
        _data = _data.loc[_data['fips'] != '46102']
        _data = _data.loc[_data['fips'] != '48999']
        _data['fips'] = _data['fips'].fillna(0)

        # Population Calculations #
        _data['population'] = [self._population_dict[_] for _ in _data['fips']]
        _data['cases_per_1k'] = ((_data['cases'] / _data['population']) * 1000).astype('float64').round(2)
        _data['deaths_per_1k'] = ((_data['deaths'] / _data['population']) * 1000).astype('float64').round(2)

        # 7 Day Smoothing #
        _data['cases_daily_avg'] = _data.groupby('fips')['cases_daily'].transform(lambda x: x.rolling(7, 1).mean())
        _data['deaths_daily_avg'] = _data.groupby('fips')['deaths_daily'].transform(lambda x: x.rolling(7, 1).mean())
        _data['cases_daily_avg'] = _data['cases_daily_avg'].astype('float64').round(2)
        _data['deaths_daily_avg'] = _data['deaths_daily_avg'].astype('float64').round(2)

        # Reformat Columns #
        _data = _data.rename(columns={'cases': 'cases_total', 'deaths': 'deaths_total'})
        _data = _data[[
            'date',
            'state',
            'county',
            'fips',
            'cases_daily',
            'deaths_daily',
            'cases_total',
            'deaths_total',
            'cases_daily_avg',
            'deaths_daily_avg',
            'cases_per_1k',
            'deaths_per_1k',
            'death_rate',
        ]]

        # List of States #
        states = _data['state'].unique().tolist()

        # Create Copy of Master Dataframe #
        self.df = _data.copy()

        # Per State Loop #
        for state in states:
            # Output #
            output_data = self.df.loc[self.df['state'] == state].fillna(0)
            output_data = output_data.reset_index(drop=True)

            # Format State Names #
            _state = str(state).replace(' ', '_').lower()

            # Add to MySQL database #
            self._printout(f'Saving {_state} Data to MySQL')
            output_data.to_sql(
                name=_state,
                con=self._mysql('covid'),
                if_exists='replace',
                index=False,
                chunksize=100,
                method='multi'
            )

            # Save CSV to Google Drive #
            self._printout(f'Saving {state} Data to HDD')
            output_data.to_csv(f'{_state_data_directory}/{_state}_covid.csv', index=False)

    # Run Main Program #
    def run(self):
        self._printout('Covid Database: By Jordan Bradley')

        # Configuration File Check #
        self._printout('Configuration Check')
        self._write_ini_params()

        # Check if Database is Running #
        self._printout('Checking if Database is Running')
        self._database_running_check()

        # Pull Population Data from US Census Bureau #
        self._printout('Population Data Check')
        if not os.path.exists(f'{self.database_directory}/population_data.csv'):
            self._population_data()
        self._population_dict = self._create_population_dict()

        # Update Weekly Google Search History #
        self._printout('Updating Google Search History')
        self._google_trends()

        # Compile Historical and Recent Case/Death Data #
        self._printout('Compiling Data')
        self._merge_data()
        self._clean_data()

        # Stop Script #
        self._printout('Database Update Complete')
        self._thread_stop()

    def __enter__(self):
        self._thread_start()

    def __exit__(self, exc_type, exc_value, tb):
        # handle exceptions with those variables ^
        self._thread_stop()


def run():
    with Covid_Database(local_directory):
        sleep(14400)
        run()


if __name__ == "__main__":
    try:
        run()
    except ConnectionResetError:
        sleep(60)
        run()
