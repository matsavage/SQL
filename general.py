# -*- coding: utf-8 -*-
"""
Created on Sat Mar 17 15:13:09 2018

@author: Mathew
"""

import sqlalchemy
import urllib

SERVER = r'MATHEW\SQLEXPRESS'
SCHEMA = 'dbo'
DATABASE = 'main'

def sql_connect(server=SERVER, database=DATABASE, **kwargs):
    '''Connect to SQL server'''
    parameters = {'driver': 'SQL Server Native Client 11.0'}
    
    string = r'mssql+pyodbc://{server}/{database}?{parameters}'
    cxn_string = string.format(server=server,
                               database=database,
                               parameters=urllib.parse.urlencode(parameters))
    return sqlalchemy.create_engine(cxn_string, **kwargs)
    
