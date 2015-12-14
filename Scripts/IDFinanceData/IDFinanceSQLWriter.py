'''
Created on Oct 26, 2015

@author: Sean
'''

import sqlite3
from IDFinanceData import IDFinanceData

class IDFinanceSQLWriter(object):
    '''
    classdocs
    '''


    def __init__(self, aDatabaseName):
        '''
        Constructor
        '''
        self.databaseconnection = sqlite3.connect(aDatabaseName)
        self.cursor = self.databaseconnection.cursor()
        
    def write(self, aIDFinanceData):
        """ write out the given finance data to the necessary database """
        if(isinstance(aIDFinanceData, IDFinanceData)):
            aIDFinanceData = [aIDFinanceData]
        
        for idfd in aIDFinanceData:
            if(idfd.data != None):
                tablename = idfd.service + idfd.ticker
                
                self.cursor.execute("SELECT * FROM sqlite_master WHERE name=? and type='table'",(tablename,))
                if(self.cursor.fetchone() == None):
                    self.cursor.execute("CREATE TABLE [{0}] (Timestamp int,Close float,High float,Low float,Open float,Volume int, UNIQUE(Timestamp))".format(tablename))
                    self.databaseconnection.commit()

                self.cursor.executemany("INSERT or IGNORE INTO [{0}] VALUES (?,?,?,?,?,?)".format(tablename), idfd.data)
                self.databaseconnection.commit()
                
    def close(self):
        self.databaseconnection.close()
            
        