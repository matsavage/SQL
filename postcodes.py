# -*- coding: utf-8 -*-
"""
Created on Sat Mar 17 17:41:11 2018

@author: Mathew
"""

import pandas as pd
from SQL import sql_connect

PATH = 'D:/Data/National_Statistics_Postcode_Lookup_UK.csv'
DATABASE = 'main'
SCHEMA = 'dbo'


def build_postcode_lookups(data, cxn=None):
    '''
    Builds lookup tables for ONS coding lookups
    '''
    names = {'Column': ['Country',
                        'County',
                        'Local Authority',
                        'Ward',
                        'Region',
                        'Parliamentary Constituency',
                        'Output Area Classification'],
             'Table': ['Countries',
                       'Counties',
                       'LocalAuthorities',
                       'Wards',
                       'Regions',
                       'Constituencies',
                       'Classifications'],
             'SQL Name': ['Country',
                          'County',
                          'LocalAuthority',
                          'Ward',
                          'Region',
                          'Constituency',
                          'Classification'],
             'KeyLength': [9, 9, 9, 9, 9, 9, 3],
             'DataLength': [20, 30, 40, 60, 25, 45, 100]}
    names = pd.DataFrame(names)

    table = '''
DROP TABLE IF EXISTS [{database}].[{schema}].[{table}];

CREATE TABLE [{database}].[{schema}].[{table}] (
        {key}        VARCHAR({keylength})   NOT NULL PRIMARY KEY CLUSTERED
      , Name         VARCHAR({datalength})  NOT NULL
        );
'''
    if cxn is None:
        cxn = sql_connect()

    for _, row in names.iterrows():
        item_data = data[['{item} Code'.format(item=row['Column']),
                          '{item} Name'.format(item=row['Column'])]].copy()
        item_data.dropna(inplace=True)
        item_data.drop_duplicates(inplace=True)
        cols = {'{item} Code'.format(item=row['Column']): row['SQL Name'],
                '{item} Name'.format(item=row['Column']): 'Name'}
        item_data.rename(columns=cols, inplace=True)

        cxn.execute(table.format(database=DATABASE,
                                 schema=SCHEMA,
                                 table=row['Table'],
                                 key=row['SQL Name'],
                                 keylength=row['KeyLength'],
                                 datalength=row['DataLength']))

        item_data.to_sql(row['Table'], cxn, schema=SCHEMA, if_exists='append',
                         index=False)

def build_postcodes(data, cxn=None):
    '''
    Builds full postcode to ONS lookup database structure including lookups
    '''
    drop_tables = '''
DROP TABLE IF EXISTS [{database}].[{schema}].[Postcodes_Staging];
DROP TABLE IF EXISTS [{database}].[{schema}].[Postcodes];
'''

    build_staging = '''
CREATE TABLE [{database}].[{schema}].[Postcodes_Staging] (
        Postcode              VARCHAR(7)      NOT NULL
      , Country               VARCHAR(9)      NULL
      , County                VARCHAR(9)      NULL
      , LocalAuthority        VARCHAR(9)      NULL
      , Ward                  VARCHAR(9)      NULL
      , Region                VARCHAR(9)      NULL
      , Constituency          VARCHAR(9)      NULL
      , Classification        VARCHAR(3)      NULL
      , MiddleSuperOutputArea VARCHAR(9)      NULL
      , LowerSuperOutputArea  VARCHAR(9)      NULL
      , Latitude              FLOAT           NULL
      , Longitude             FLOAT           NULL
      , PositionalQuality     INT             NOT NULL
      , Introduced            VARCHAR(15)     NOT NULL
      , Uploaded              VARCHAR(15)     NOT NULL
);
'''

    build_final = '''
DROP TABLE IF EXISTS [{database}].[{schema}].[Postcodes];
CREATE TABLE [{database}].[{schema}].[Postcodes] (
        Postcode              VARCHAR(7)      NOT NULL    PRIMARY KEY CLUSTERED
      , Country               VARCHAR(9)      NULL        FOREIGN KEYREFERENCES Countries(Country)
      , County                VARCHAR(9)      NULL        FOREIGN KEY REFERENCES Counties(County)
      , LocalAuthority        VARCHAR(9)      NULL        FOREIGN KEY REFERENCES LocalAuthorities(LocalAuthority)
      , Ward                  VARCHAR(9)      NULL        FOREIGN KEY REFERENCES Wards(Ward)
      , Region                VARCHAR(9)      NULL        FOREIGN KEY REFERENCES Regions(Region)
      , Constituency          VARCHAR(9)      NULL        FOREIGN KEY REFERENCES Constituencies(Constituency)
      , MiddleSuperOutputArea VARCHAR(9)      NULL
      , LowerSuperOutputArea  VARCHAR(9)      NULL
      , Classification        VARCHAR(3)      NULL        FOREIGN KEY REFERENCES Classifications(Classification)
      , Location              GEOGRAPHY       NOT NULL
      , PositionalQuality     TINYINT         NOT NULL
      , Introduced            DATE            NOT NULL
      , Uploaded              DATE            NOT NULL
        );
INSERT INTO [{database}].[{schema}].[Postcodes]
SELECT
        Postcode
      , Country
      , County
      , LocalAuthority
      , Ward
      , Region
      , Constituency
      , MiddleSuperOutputArea
      , LowerSuperOutputArea
      , Classification
      , geography::Point(Latitude, Longitude, 4326) AS Location
      , PositionalQuality
      , CAST('01-' + Introduced AS DATE) AS Introduced
      , CAST(Uploaded AS DATE) AS Uploaded
FROM [{database}].[{schema}].[Postcodes_Staging];
DROP TABLE IF EXISTS [{database}].[{schema}].[Postcodes_Staging];
'''

    if cxn is None:
        cxn = sql_connect()

    cxn.execute(drop_tables.format(database=DATABASE, schema=SCHEMA))
    build_postcode_lookups(data, cxn=cxn)


    cxn.execute(build_staging.format(database=DATABASE, schema=SCHEMA))
    columns = {'Postcode': 'Postcode',
               'Country Code': 'Country',
               'County Code': 'County',
               'Local Authority Code': 'LocalAuthority',
               'Ward Code': 'Ward',
               'Region Code': 'Region',
               'Parliamentary Constituency Code': 'Constituency',
               'Lower Super Output Area Code': 'LowerSuperOutputArea',
               'Middle Super Output Area Code': 'MiddleSuperOutputArea',
               'Output Area Classification Code': 'Classification',
               'Positional Quality': 'PositionalQuality',
               'Longitude': 'Longitude',
               'Latitude': 'Latitude',
               'Date Introduced': 'Introduced',
               'Last Uploaded': 'Uploaded'}
    postcode_data = data[list(columns.keys())].copy()
    postcode_data.rename(columns=columns, inplace=True)

    postcode_data.to_sql('Postcodes_Staging', cxn, schema=SCHEMA,
                         if_exists='append', index=False)
    cxn.execute(build_final.format(database=DATABASE, schema=SCHEMA))


def main():
    '''
    Main function to read in ONS statistics lookup and generate database
    structure
    '''
    data = pd.read_csv(PATH)
    data.drop(data[data['Latitude'] == 99.999999].index, inplace=True)
    data['Postcode'] = data['Postcode 1'].apply(lambda x: x.replace(' ', ''))
    data.loc[data['Region Code'] == 'W99999999', 'Region Name'] = 'Wales'
    data.loc[data['Region Code'] == 'S99999999', 'Region Name'] = 'Scotland'
    data.loc[data['Region Code'] == 'N99999999', 'Region Name'] = 'Northern Ireland'

    cxn = sql_connect()

    build_postcodes(data, cxn=cxn)


if __name__ == '__main__':
    main()
