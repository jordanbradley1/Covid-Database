#!/usr/bin/env python

import os
import datetime
from time import sleep
import subprocess
import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from threading import Thread
from shutil import get_terminal_size
from pytrends.request import TrendReq
import configparser

########################################################################################################################

# Pandas Formatting Options #
pd.set_option('use_inf_as_na', True)
print_options = pd.option_context(
    'display.max_rows', None,
    'display.max_columns', None,
    'display.width', None,
    'display.max_colwidth', None
)

########################################################################################################################

# Check if Database Directory is already located on C Drive #
local_directory = Path(f'C:/COVID19/')
if not os.path.isdir(local_directory):
    os.mkdir(local_directory)


########################################################################################################################

class config_handler:
    def __init__(self):
        # Configuration Variables #
        self.config = local_directory / 'covid19_config.ini'
        self.write_config = configparser.ConfigParser(strict=False)
        self.read_config = configparser.ConfigParser(strict=False)

    def _config_check(self):
        """ Check for Config File, creates one from inputs if none is found """
        if not os.path.isfile(self.config):
            with open(self.config, 'a') as config:
                self.write_config.write(config)
            config.close()

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
            subprocess.Popen(["D:/xampp/xampp-control.exe"], shell=True)
            sleep(2)

            # Restart #
            self.run()

    def _write_ini_params(self):
        """ Write Parameters to Configuration File """
        self.read_config.read(self.config)

        # write database location #
        for section in ['database']:
            if not self.read_config.has_section(section):
                self.write_config.add_section('database')

                for option in ['location']:
                    if not self.read_config.has_option(section, option):
                        self.write_config[str(section)][str(option)] = str(local_directory)

            else:
                pass

        # write mysql settings #
        for section in ['mysql']:
            if not self.read_config.has_section(section):
                mysql_check = input("Save Data to MySQL (Y/N): ").lower()
                valid_inputs = ['y', 'n']

                if mysql_check not in valid_inputs:
                    print('Invalid Input')
                    self._write_ini_params()
                if str(mysql_check).lower() == 'n':
                    pass
                if str(mysql_check).lower() == 'y':
                    self.write_config.add_section('mysql')
                    host = input("Host: ")
                    root = input("Root: ")
                    for option in ['host', 'user']:
                        if not self.read_config.has_option(section, option):
                            self.write_config[str(section)]['host'] = host
                            self.write_config[str(section)]['user'] = root
                            # self.write_config[str(section)]['host'] = 'localhost'
                            # self.write_config[str(section)]['user'] = 'root'

                    self._database_running_check()

                else:
                    pass

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

        # Write Information to .ini File #
        with open(self.config, 'a') as f:
            self.write_config.write(f)

    def run(self):
        self._config_check()
        self._write_ini_params()
        return self.config, self.write_config, self.read_config


########################################################################################################################

class Covid_Database:
    def __init__(self):
        """
        Pull New York Times Covid Case/Death Data per State/County and Store Locally and in MySQL Database
        """

        # Now Datetime #
        self.now = f'{datetime.datetime.now():%Y-%m-%d %H:%M:%S}'

        # Leading Text of Printout #
        self.text_header = 'Current Operation: '

        # Location of Main Database CSV backups #
        self.database_directory = local_directory

        # Blank objects to store data #
        self.df = pd.DataFrame()
        self.dict = {}
        self.population_dict = {}

        # State Abbreviation Dictionary #
        self.states = {
            'US': 'United States',
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

        # Multithread Handling #
        self.thread_ = Thread(target=self.run, daemon=True)
        self.done = False

        self.mysql = False

    def _use_mysql(self):
        self.config = local_directory / 'covid19_config.ini'
        self.read_config = configparser.ConfigParser(strict=False)

        for section in ['mysql']:
            if self.read_config.has_section(section):
                return True
            else:
                return False

    # Thread Starter #
    def _thread_start(self):
        self.thread_.start()
        return self

    # Thread Stopper #
    def _thread_stop(self):
        self.done = True
        cols = get_terminal_size((80, 20)).columns
        self._printout(f"\rCompleted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}" + " " * cols)

    # Function to simplify the printouts #
    def _printout(self, text):
        print(f'\r{self.text_header} {text}', flush=True, end="")
        sleep(1)

    # Setup MySQL Connection #
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
            columns={'FIPStxt': 'fips', 'State': 'state', 'Area name': 'county', 'Value': 'population'}
        )

        # Land Area Data #
        land_area = pd.read_excel(
            'https://www2.census.gov/library/publications/2011/compendia/usa-counties/excel/LND01.xls',
            usecols=['STCOU', 'LND010200D']
        )

        # Rename Columns #
        land_area = land_area.rename(columns={'STCOU': 'fips', 'LND010200D': 'land_area'})

        land_area['fips'] = land_area['fips'].replace(46113, 46102).replace()

        # Merge Population and Land Area #
        merged_data = data.merge(land_area, how='inner', on=['fips'])

        # Calculate population density #
        merged_data['density'] = round(merged_data['population'] / merged_data['land_area'], 2)
        merged_data['state'] = merged_data['state'].replace(self.states).str.upper()
        merged_data['county'] = merged_data['county'].str.upper()
        merged_data = merged_data.fillna(0)

        # Format Data #
        merged_data['fips'] = merged_data['fips'].astype(str)
        merged_data['fips'] = merged_data['fips'].str.zfill(5)
        merged_data = merged_data.astype(
            {
                'fips': 'string',
                'state': 'string',
                'county': 'string',
                'population': 'int32',
                'land_area': 'float64',
            }
        )

        if self.mysql:
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
        self.df = self.df.astype(
            {
                'date': 'datetime64[D]',
                'state': 'string',
                'google_trend': 'int16'
            }
        )

        if self.mysql:
            # Add to MySQL database #
            self.df.to_sql(
                name='google_trends',
                con=self._mysql('google_trend'),
                if_exists='replace',
                index=False,
                chunksize=100,
                method='multi',
            )

        # Save to CSV #
        self.df.to_csv(self.database_directory / 'google_trend_data.csv', index=True)

    # Get State Vaccination Data #
    def _vaccine_data(self):
        _data = pd.read_csv('https://data.cdc.gov/api/views/unsk-b7fc/rows.csv')
        _data = _data[['Date', 'Location', 'Administered']]
        _data = _data.rename(columns={'Date': 'date', 'Location': 'state', 'Administered': 'administered'})
        _data = _data.loc[_data['state'] != 'VI']
        _data = _data.loc[_data['state'] != 'MH']
        _data = _data.loc[_data['state'] != 'IH2']
        _data = _data.loc[_data['state'] != 'PR']
        _data = _data.loc[_data['state'] != 'PW']
        _data = _data.loc[_data['state'] != 'VA2']
        _data = _data.loc[_data['state'] != 'BP2']
        _data = _data.loc[_data['state'] != 'GU']
        _data = _data.loc[_data['state'] != 'MP']
        _data = _data.loc[_data['state'] != 'FM']
        _data = _data.loc[_data['state'] != 'DD2']
        _data = _data.loc[_data['state'] != 'LTC']
        _data = _data.loc[_data['state'] != 'RP']
        _data = _data.loc[_data['state'] != 'AS']
        _data['state'] = _data['state'].map(self.states).fillna(_data['state'])
        _data['state'] = _data['state'].str.upper()
        _data = _data.astype(
            {
                'date': 'datetime64[D]',
                'state': 'string',
                'administered': 'int64'
            }
        )
        _data['date'] = pd.to_datetime(_data['date'], format='%Y-%m-%d')

        self._printout(f'Saving Vaccination Data to MySQL')

        if self.mysql:
            _data.to_sql(
                name='vaccine',
                con=self._mysql('vaccine'),
                if_exists='replace',
                index=False,
                chunksize=100,
                method='multi'
            )

        # Save CSV to Google Drive #
        self._printout(f'Saving Vaccination Data to HDD')
        _data.to_csv(self.database_directory / 'vaccine_data.csv', index=False)

    # Get Historical Data #
    @staticmethod
    def _get_historical_data():
        # Historical Data Variable #
        historical_url = f'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'

        # Create DataFrame from Data #
        historical_data = pd.read_csv(historical_url, dtype=object)

        historical_data = historical_data.astype(
            {
                'date': 'datetime64[D]',
                'county': 'string',
                'state': 'string',
            }
        )

        return historical_data

    # Get Live Data #
    @staticmethod
    def _get_live_data():
        # Create DataFrame object from live data #
        live_data = pd.read_csv(
            f'https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/us-counties.csv',
            usecols=[0, 1, 2, 3, 4, 5], dtype=object)
        live_data = live_data.astype(
            {
                'date': 'datetime64[D]',
                'county': 'string',
                'state': 'string',
            }
        )
        return live_data

    # Merge Historical and Live Data #
    def _merge_data(self):
        _historical_data = self._get_historical_data()
        _live_data = self._get_live_data()

        # Merge Historical and Live Data #
        self._printout('Merging Data')
        _data = pd.concat([_live_data, _historical_data])

        return _data

    # Clean Results #
    def _clean_data(self):
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

        # ## EDIT OUT, PULLS SAMPLE FOR TABLEAU ##
        # _data['date'] = pd.to_datetime(_data['date'])
        # _data = _data[_data["date"].isin(pd.date_range("2021-09-01", "2022-02-01"))]
        # ######

        # Remove Unknown Fips Values #
        _data = _data.loc[_data['fips'] != np.NaN]
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
        _us_data = _data.groupby(['date']).agg({'cases': 'sum', 'deaths': 'sum'}).reset_index()
        _us_data['state'] = 'UNITED STATES'
        _us_data['county'] = 'UNITED STATES'
        _us_data['fips'] = '00000'

        _data = pd.concat([_data, _us_data], ignore_index=True)
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
        _data = _data.loc[_data['fips'] != '48999']
        _data['fips'] = _data['fips'].fillna(0)

        # Population Calculations #
        _data['population'] = [self.population_dict[_] for _ in _data['fips']]
        _data['cases_per_1k'] = ((_data['cases'] / _data['population']) * 1000).astype('float64').round(2)
        _data['deaths_per_1k'] = ((_data['deaths'] / _data['population']) * 1000).astype('float64').round(2)

        # 7 Day Smoothing #
        _data['cases_daily_avg'] = _data.groupby('fips')['cases_daily'].transform(lambda x: x.rolling(14, 1).mean())
        _data['deaths_daily_avg'] = _data.groupby('fips')['deaths_daily'].transform(lambda x: x.rolling(14, 1).mean())
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
        _data['date'] = pd.to_datetime(_data['date'], format='%Y-%m-%d')

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
            self._printout(f'Saving {state} Data to MySQL')

            if self.mysql:
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
        print("\nCovid_Database_0.0.2 by Jordan Bradley\n")

        # Configuration #
        config_handler().run()
        self.mysql = self._use_mysql()

        # Pull Population Data from US Census Bureau #
        self._printout('Population Data')
        self._population_data()
        self.population_dict = self._create_population_dict()

        # Update Weekly Google Search History #
        self._printout('Updating Google Search History')
        self._google_trends()

        # Update Vaccine Data #
        self._printout('Updating Vaccine Data')
        self._vaccine_data()

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


if __name__ == "__main__":
    try:
        Covid_Database().run()
    except ConnectionResetError:
        sleep(300)
        Covid_Database().run()
