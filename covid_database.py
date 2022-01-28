#!/usr/bin/env python

import os
import datetime
from time import sleep
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine
from threading import Thread
from shutil import get_terminal_size

# Pandas Option to set Infinity values as NaN
pd.set_option('use_inf_as_na', True)


# Set Covid Database Parameters #
class variables(object):

    def __init__(self):
        self.print_options = pd.option_context(
            # 'display.max_rows', None,
            'display.max_columns', None,
            'display.width', None,
            'display.max_colwidth', None
        )

        # Printout Leading Text #
        self.lead_text = 'Current Operation: '

        # Now Datetime #
        self.now = f'{datetime.datetime.now():%Y-%m-%d %H:%M:%S}'

        # MySQL Connection #
        self.my_conn = create_engine(
            "mysql+mysqlconnector://root:@localhost/covid",
            connect_args={'connect_timeout': 600},
        )

        # Create Database Directory #
        self.database_directory = self._database_directory(path=f'I:/My Drive/covid_database/')

    # Create Database Directory to Google Drive #
    @staticmethod
    def _database_directory(path):
        """
        Check if Directory Exists, if not, one is created
        :rtype: Directory to Google Drive Cloud
        """
        database_location = Path(path)

        # Create directory if one does not exist #
        if not os.path.exists(database_location):
            os.makedirs(database_location)

        # returns location as path #
        return database_location


# Build Covid Database per State #
class Covid_Database(object):
    """ Pull New York Times Covid Case/Death Data per State/County and Store in MySQL """

    def __init__(self):

        # Call Variables from v variable #
        self.variables = variables()

        # Location of Main Database CSV backups #
        self.database = self.variables.database_directory

        # Lead Text of Printout #
        self.lead_text = self.variables.lead_text

        # Variable handling multithreading #
        self.thread_ = Thread(target=self.run, daemon=True)
        self.done = False

        # Population Dictionary #
        self.population_dict = self._create_population_dict()
        # Blank Dataframe used to store data #
        self.df = pd.DataFrame()
        self.dict = {}

    # Thread Starter #
    def _thread_start(self):
        self.thread_.start()
        return self

    # Get Population per FIPS Code #
    def _fips_population(self):
        """
        Pulls Population Data per FIPS code from GitHub
        :rtype: Dataframe Object, CSV File
        """

        # Check if fips csv already exists #
        if not os.path.exists(f'{self.database}/us_fips.csv'):
            # Download CSV from GitHub #
            fips_pop = pd.read_csv(
                f'https://raw.githubusercontent.com/jordanbradley1/covid_database/main/us_fips.csv',
                dtype=object,
            )

            # keep leading zeroes on fips #
            fips_pop['fips'] = fips_pop['fips'].str.zfill(5)

            # Save Data to Database Directory #
            fips_pop.to_csv(f'{self.database}/us_fips.csv')

    # Create Population Dictionary #
    def _create_population_dict(self):
        _pop_df = pd.read_csv(f'{self.database}/us_fips.csv', index_col=0, dtype=object)
        _population_dict = dict(zip(_pop_df['fips'].astype(str), _pop_df['population'].astype(int)))
        return _population_dict

    # Function to Pull Population Data #
    def _get_population(self, fips):
        """ create population dictionary """
        try:
            return int(self.population_dict[fips])
        except KeyError:
            return np.NAN

    # Get Historical COVID-19 Data #
    @staticmethod
    def _get_historical_data():
        """
        Returns Historical Covid Data (Cases/Deaths)
        :rtype: Dataframe Object
        """

        # Historical Data Variable #
        historical_url = f'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'

        # Create DataFrame from Data #
        historical_data = pd.read_csv(historical_url, dtype=object)

        return historical_data

    # Get Live Data #
    @staticmethod
    def _get_live_data():
        """
        Get Most Recent Daily Data
        :rtype: DataFrame Object
        """

        # Create DataFrame object from live data #
        live_data = pd.read_csv(
            f'https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/us-counties.csv',
            usecols=[0, 1, 2, 3, 4, 5], dtype=object)

        return live_data

    # Function to simplify the printouts #
    def _printout(self, text):
        print(f'\r{self.lead_text} {text}', flush=True, end="")

    # Run Main Program #
    def run(self):
        """
        Combines all _data into ones DataFrame for Analysis
        :rtype: DataFrame
        """

        # Starting Program #
        self._printout('Starting Covid Database')
        ######

        # Data Collection #
        self._printout('Checking if FIPS file exists')
        self._fips_population()
        ######

        self._printout('Collecting Historical Data')
        _historical_data = self._get_historical_data()
        ######

        self._printout('Collecting Recent Data')
        _live_data = self._get_live_data()
        ######

        # Merge Historical and Live Data #
        self._printout('Merging Data')
        _data = pd.concat([_live_data, _historical_data])
        ######

        # Remove Unknown Fips Values #
        _data = _data.loc[_data['state'] != 'Guam']
        _data = _data.loc[_data['state'] != 'Northern Mariana Islands']
        _data = _data.loc[_data['state'] != 'Virgin Islands']
        _data = _data.loc[_data['state'] != 'American Samoa']
        _data = _data.loc[_data['state'] != 'Puerto Rico']
        _data = _data.loc[_data['county'] != 'Unknown']
        _data = _data.loc[_data['fips'] != '00nan']
        ######

        # Format State and County names to Uppercase #
        _data['state'] = _data['state'].str.upper()
        _data['county'] = _data['county'].str.upper()
        ######

        # Delete Duplicates and Sort #
        self._printout('Removing Duplicates and Sorting')
        _data = _data.drop_duplicates(ignore_index=True)
        _data = _data.sort_values(by=['state', 'county', 'date'])
        _data = _data.reset_index(drop=True)
        ######

        # Format Data for Extra Calculations #
        self._printout('Data Conversion')
        _data['cases'] = _data['cases'].fillna(0).astype('int32')
        _data['deaths'] = _data['deaths'].fillna(0).astype('int32')
        _data['date'] = pd.to_datetime(_data['date'])
        ######

        # Calculate Daily Cases/Deaths and other various calculations #
        self._printout('Additional Calculations')
        _data["cases_daily"] = _data.groupby('fips')["cases"].diff(1)
        _data["deaths_daily"] = _data.groupby('fips')["deaths"].diff(1)
        _data["cases_daily"] = _data["cases_daily"].fillna(0).astype('int32')
        _data["deaths_daily"] = _data["deaths_daily"].fillna(0).astype('int32')
        ######

        # Infected Death Rate #
        _data['death_rate'] = (_data['deaths'] / _data['cases']).fillna(0)
        _data['death_rate'] = _data['death_rate'].round(4)
        ######

        # Per 1k #
        _data['fips'] = _data['fips'].astype(str).str.zfill(5)
        _data['population'] = [self._get_population(_) for _ in _data['fips']]
        _data['cases_per_1k'] = ((_data['cases'] / _data['population']) * 1000).astype('float64').round(2)
        _data['deaths_per_1k'] = ((_data['deaths'] / _data['population']) * 1000).astype('float64').round(2)
        #####

        # 7 Day Smoothing #
        _data['cases_daily_avg'] = _data.groupby('fips')['cases_daily'].transform(lambda x: x.rolling(7, 1).mean())
        _data['deaths_daily_avg'] = _data.groupby('fips')['deaths_daily'].transform(lambda x: x.rolling(7, 1).mean())
        _data['cases_daily_avg'] = _data['cases_daily_avg'].astype('float64').round(2)
        _data['deaths_daily_avg'] = _data['deaths_daily_avg'].astype('float64').round(2)
        ######

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
        ######

        # Create Copy of Master Dataframe #
        self.df = _data.copy()
        ######

        # List of States #
        states = _data['state'].unique().tolist()

        # Per State For Loop #
        for state in states:
            # Format State Names #
            _state = str(state).replace(' ', '_').lower()

            # Output #
            output_data = self.df.loc[self.df['state'] == state].fillna(0)
            output_data = output_data.reset_index(drop=True)

            # Add to MySQL database #
            self._printout(f'Saving {state} Data to MySQL')
            output_data.to_sql(
                name=_state,
                con=self.variables.my_conn,
                if_exists='replace',
                index=False,
                chunksize=100,
                method='multi'
            )
            ######

            # Save CSV to Google Drive #
            self._printout(f'Saving {state} Data to HDD')
            output_data.to_csv(f'{self.database}/State_Data/{_state}_covid.csv', index=False)
            ######

        # Stop Script #
        self.stop()

    def __enter__(self):
        self._thread_start()

    def stop(self):
        self.variables.my_conn.dispose()
        self.done = True
        cols = get_terminal_size((80, 20)).columns
        self._printout(f"\rCompleted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}" + " " * cols)

    def __exit__(self, exc_type, exc_value, tb):
        # handle exceptions with those variables ^
        self.stop()


def run():
    with Covid_Database():
        sleep(14400)
        run()


if __name__ == "__main__":
    try:
        run()
    except ConnectionResetError:
        sleep(60)
        run()
