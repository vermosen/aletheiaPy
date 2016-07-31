'''
Created on Jul 15, 2016

@author: jean-mathieu vermosen
'''

import sys
import quandl as ql
import pandas as pd
import datetime as dt

from pandas.io.data import DataReader

from aletheia import database as db
from aletheia.recordsets import timeseries, quandl, yahoo


def main():
    
    myDb = db.database(dict(db          = 'aletheia' ,
                            user        = 'postgres' ,
                            password    = '1234'     ,
                            host        = 'localhost',
                            port        = '5432'     ))
    try:
        print('welcome to aletheia db manager v0.1\r\n\r\n')
        
        recurseMenu(myDb, None)
    except Exception as e:
        print('fatal error: ' + str(e) + '\r\n')
    finally:
        print ('goodbye !\r\n')
    
def recurseMenu(db, errorMessage = None):
    
    options = {1 : insertNewIndex,
               2 : refreshDatabase,
               3 : readDataset,
               4 : test,
               5 : rebuildDb}
    
    if (errorMessage != None):
        print(errorMessage + '\n')
        
    print('please select an activity:')
    print('1 - insert a new index')
    print('2 - refresh the database')
    print('3 - read a dataset')
    print('4 - [debug] test')
    print('5 - rebuild the database')
    print('0 - exit')

    choice = 0    
    # try parse
    try:
        choice = int(input(''))
    except Exception as e:
        recurseMenu(db, 'invalid choice !')
        
    if(choice == 0):
        return
    else:
        if choice in options:
            options[choice](db)
        else:
            recurseMenu(db, 'invalid choice !')

def insertNewIndex(db, errorMessage = None):
    
    options = {1 : insertNewQuandlIndex,
               2 : insertNewYahooIndex,
               3 : insertNewPepperstoneIndex}
    
    if (errorMessage != None):
        print(errorMessage + '\n')
        
    print('please select a source:')
    print('1 - Quandl')
    print('2 - Yahoo Finance')
    print('3 - Pepperstone')
    print('0 - return to main menu')
    
    choice = 0    
    # try parse
    try:
        choice = int(input(''))
    except Exception as e:
        insertNewIndex(db, 'invalid choice !')
        
    if(choice == 0):
        recurseMenu(db)
    else:
        if choice in options:
            options[choice](db)
        else:
            recurseMenu(db, 'invalid choice !')

def insertNewYahooIndex(db):
    
    print("Please provide a key:")
    key = input('')
    print("Please provide an index:")
    idx = input('')
    print("Please provide a description:")
    des = input('')
    
    try:
        yahoo.yahooRecordset(db).insert((key, idx, des))
        insertNewIndex(db, None)
    except Exception as e:
        insertNewIndex(db, key + '/' + idx + \
           ' is not a valid selection: ' + str(e))
        
def insertNewQuandlIndex(db):
    
    print("Please provide a catalog:")
    cat = input('')
    print("Please provide a key:")
    key = input('')
    print("Please provide an index:")
    idx = input('')
    print("Please provide a description:")
    des = input('')
    
    try:
        quandl.quandlRecordset(db).insert((key, cat, idx, des))
        insertNewIndex(db, None)
    except Exception as e:
        insertNewIndex(db, cat + '/' + key + '/' + idx + \
           ' is not a valid selection: ' + str(e))
    
def insertNewPepperstoneIndex(db):    
    insertNewIndex(db, "not implemented yet...")

def refreshQuandlDatabase(db):
    
    rs = quandl.quandlRecordset(db)
    ts = timeseries.timeseriesRecordset(db)
    
    # step 1: get the quandl index end date
    idx = rs.select('*')
    
    for i in idx:
        
        try:
            r = ts.getLastDate(i['id'])
            
            start = r[0]['max_date']
            
            if start != None:
                start = start + dt.timedelta(days=1)
            
            # advance the date by 1 day
            
            
            data = ql.get(  dataset     = str(i['catalog']) + '/' + str(i['key']),
                            authtoken   = 'H8VUjcUPEFHK_mFnjXp1',
                            start_date  = start                 ,
                            returns     = 'pandas'              ,
                            order       = 'asc'                 )
            
            # insert new data
            add = []
            
            for index, row in data.iterrows():
                add.append((i['id'], index, row[i['field']]))
                
            ts.insert(add)
            
        except Exception as e:
            print('error processing index ' + str(i['catalog']) \
                + '/' + str(i['key'] + '/' + str(i['field']) + '\n'))
    
def refreshYahooDatabase(db):
    
    rs = yahoo.yahooRecordset(db)
    ts = timeseries.timeseriesRecordset(db)
    
    # step 1: get the yahoo index end date
    idx = rs.select('*')
    
    for i in idx:
        
        try:
            r = ts.getLastDate(i['id'])
            
            start = r[0]['max_date']
            
            data = pd.DataFrame()
            
            if start != None:
                start = start + dt.timedelta(days=1)
                
                data = DataReader(  str(i['key']), 'yahoo', start)
            else:
                data = DataReader(  str(i['key']), 'yahoo')
            
            # insert new data
            add = []
            
            for index, row in data.iterrows():
                add.append((i['id'], index, row[i['field']]))
                
            ts.insert(add)
            
        except Exception as e:
            print('error processing index ' + str(i['key'] \
                + '/' + str(i['field']) + '\n'))
    
def refreshDatabase(db):
    
    try:
        refreshQuandlDatabase(db)
        refreshYahooDatabase(db)
        recurseMenu(db, None)
        
    except Exception as e:
        recurseMenu(db, 'an error occurred while refreshing database: ' \
            + str(e) +'\n')
    
    
def readDataset(db):
    print("bla\n")

def test(db):
    
    start = dt.datetime(2016, 1, 1)
    data = DataReader("IBM", 'yahoo', start)

    add = []
            
    for index, row in data.iterrows():
        add.append(("IBM", index, row["Adj Close"]))

    recurseMenu(db)
    
def rebuildDb(db):

    # confirmation
    inp = input("that command will erase all the data, do you want to continue ? (y/n)\n")
    
    while inp != str('y') and inp != str('n'):
        inp = input('invalid choice, please try again:\n')
        
    if inp == 'y': 
        try:
            db.dropAll()
            db.rebuild()
            
            print('successfully recreated the database\n')
            
        except Exception as e: 
            recurseMenu(db, "Unexpected error: " + str(e))
    
    recurseMenu(db)
    
if __name__ == "__main__":
    main()
    