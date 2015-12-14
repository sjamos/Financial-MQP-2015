'''
Created on Nov 12, 2015

@author: Sean
'''

from builtins import str, int
import pickle
import queue
import sqlite3
import threading
import fractions
import newspaper
import os

import Logger
from _functools import reduce


module_logging = Logger.getLogger(__name__)

class ValueTimes:
    def __init__(self, aValueTimes):
        self.valuetimes = aValueTimes
      
    def isZero(self):
        ret = True
        for word in self.valuetimes:
            ret = ret and (self.valuetimes[word] == 0 or self.valuetimes[word] == 0.0)
        return ret
    
    @staticmethod  
    def loads(aPickleString):
        if aPickleString == None:
            return None
        return ValueTimes( pickle.loads(aPickleString) )
        
    def dumps(self):
        return pickle.dumps(self.valuetimes)
    
    def __add__(self, other):
        if other == None:
            return ValueTimes(self.valuetimes)
        if isinstance(other, str):
            other = self.loads(other)
        if isinstance(other, ValueTimes):
            retvaluetimes = {}
            for atime in self.valuetimes:
                if atime in other.valuetimes:
                    retvaluetimes[atime] = self.valuetimes[atime] + other.valuetimes[atime]
            return ValueTimes(retvaluetimes)
        else:
            return None
        
    def __sub__(self, other):
        if other == None:
            return ValueTimes(self.valuetimes)
        if isinstance(other, str):
            other = self.loads(other)
        if isinstance(other, ValueTimes):
            retvaluetimes = {}
            for atime in self.valuetimes:
                if atime in other.valuetimes:
                    retvaluetimes[atime] = self.valuetimes[atime] - other.valuetimes[atime]
            return ValueTimes(retvaluetimes)
        else:
            return None
    
    def __mul__(self, other):
        if isinstance(other, int) or isinstance(other, float):
            retvaluetimes = {}
            for atime in self.valuetimes:
                retvaluetimes[atime] = self.valuetimes[atime] * float(other)
            return ValueTimes(retvaluetimes)
        else:
            return None
        
    def __getitem__(self, key):
        return self.valuetimes[int(key)]
    
    def __iter__(self):
        return self.valuetimes.__iter__()
    
    def __contains__(self, item):
        return item in self.valuetimes
    
    def keys(self):
        return sorted(list(self.valuetimes.keys()))
    
    def __len__(self):
        return len(self.valuetimes)
    
    def GCD(self):
        gcd = reduce( fractions.gcd, self.valuetimes)
        return gcd
                 
class ThreadManager:
    stopTokenPriority = 1
    regularPriority = 2
    
    def __init__(self, aName, aThreadCount, aTarget, aQueue=None, aStopToken="STOP"):
        self.status = "OPEN"
        self.threads = []
        self.queue = queue.PriorityQueue()
        self.stoptoken = aStopToken
            
        for i in range(aThreadCount):
            self.threads.append( threading.Thread(target=aTarget, name="{0}-{1}".format(aName, i), args=(self.queue, self.stoptoken,)) )
            self.threads[-1].start()   
            
    def put(self, item):
        self.queue.put((self.regularPriority, item))
        
    def get(self, block=True, timeout=None):
        return self.queue.get(block, timeout)[1]
    
    def join(self):
        self.queue.join()
        
    def close(self):
        if(self.status != "CLOSED"):
            self.status = "CLOSED"

            for _ in self.threads:
                self.queue.put((self.stopTokenPriority, self.stoptoken))
                
            for thread in self.threads:
                thread.join()
        
    def __del__(self):
        self.close()
        
class SQLConnection:

    def __init__(self, aDatabaseName):
        self.status = "OPEN"
        self.databasename = aDatabaseName
        self.customFunction = []
        self.dbconnection = sqlite3.connect(self.databasename)
        self.dbcursor = self.dbconnection.cursor()
        
    def addCustomFunction(self, aCustomSQLFunction):
        if aCustomSQLFunction not in self.customFunction:
            self.customFunction.append(aCustomSQLFunction)
            
    def execute(self, sql, parameters=()):
        ret = None
        for customfunc in self.customFunction:
            response = customfunc.intercept(self.dbcursor, sql, parameters)
            if response[0]:
                ret = response[1]
                break
        else:
            try:
                self.dbcursor.execute(sql, parameters)
                ret = self.dbcursor.fetchall()
            except Exception as e:
                module_logging.exception(e)
                module_logging.exception(sql)
        
        self.dbconnection.commit()
        return ret
        
    def close(self):
        if(self.status != "CLOSED"):
            self.status = "CLOSED"
            self.dbconnection.close()
        
    def __del__(self):
        self.close()          


class memorySQLConnection:

    def __init__(self, aDatabaseName):
        self.status = "OPEN"
        self.databasename = aDatabaseName
        self.customFunction = []
        temp_connect = sqlite3.connect(self.databasename)
        self.dbconnection = self.__copy_database(temp_connect)
        self.dbcursor = self.dbconnection.cursor()
        
    def addCustomFunction(self, aCustomSQLFunction):
        if aCustomSQLFunction not in self.customFunction:
            self.customFunction.append(aCustomSQLFunction)
            
    def execute(self, sql, parameters=()):
        ret = None
        for customfunc in self.customFunction:
            response = customfunc.intercept(self.dbcursor, sql, parameters)
            if response[0]:
                ret = response[1]
                break
        else:
            try:
                self.dbcursor.execute(sql, parameters)
                ret = self.dbcursor.fetchall()
            except Exception as e:
                module_logging.exception(e)
                module_logging.exception(sql)
        
        self.dbconnection.commit()
        return ret
        
    def close(self):
        if(self.status != "CLOSED"):
            self.status = "CLOSED"
            self.__copy_database(self.dbconnection, self.databasename)
            self.dbconnection.close()
        
    def __del__(self):
        self.close()     
        
    def __copy_database(self, source_connection, dest_dbname=':memory:'):
        '''Return a connection to a new copy of an existing database.                        
           Raises an sqlite3.OperationalError if the destination already exists.             
        '''
        script = ''.join(source_connection.iterdump())
        if dest_dbname!=':memory:':
            os.remove(dest_dbname)
        dest_conn = sqlite3.connect(dest_dbname)
        dest_conn.executescript(script)
        return dest_conn
                
class TSSQLConnection:
    stopToken = "STOP"
    
    def __init__(self, aDatabaseName):
        self.status = "OPEN"
        self.databasename = aDatabaseName
        self.returnvalue = None
        self.workqueue = queue.Queue(1)
        self.executelock = threading.RLock()
        self.workerthread = threading.Thread(target=self.worker, name=self.databasename + "-Worker")
        self.finishflag = threading.Event()
        self.workerthread.start()
        self.customFunction = []
        
    def addCustomFunction(self, aCustomSQLFunction):
        if aCustomSQLFunction not in self.customFunction:
            self.customFunction.append(aCustomSQLFunction)
            
    def execute(self, sql, parameters=()):
        self.returnvalue = None
        with self.executelock:
            self.finishflag.clear()
            self.workqueue.put( (sql, parameters) )
            self.finishflag.wait()
        return self.returnvalue
    
    def worker(self):
        dbconnection = sqlite3.connect(self.databasename)
        dbcursor = dbconnection.cursor()
        
        while True:
            sqlset = self.workqueue.get()
            if sqlset == self.stopToken:
                break
            
            for customfunc in self.customFunction:
                response = customfunc.intercept(dbcursor, sqlset[0], sqlset[1])
                if response[0]:
                    self.returnvalue = response[1]
                    break
            else:
                try:
                    dbcursor.execute(sqlset[0], sqlset[1])
                except Exception as e:
                    module_logging.exception(e)
                    module_logging.exception(sqlset[0])

            self.returnvalue = dbcursor.fetchall()
            
            dbconnection.commit()
            self.workqueue.task_done()
            self.finishflag.set()
            
        dbconnection.close()
        self.workqueue.task_done()
        
    def close(self):
        if(self.status != "CLOSED"):
            self.status = "CLOSED"
            self.workqueue.put(self.stopToken)
            self.workerthread.join()
        
    def __del__(self):
        self.close()    
 
class CustomSQLFunction:
    def __init__(self, aTSSQLConnection, aInterceptFunc):
        self.workinglock = threading.RLock()
        with self.workinglock:
            self.connection = aTSSQLConnection
            self.connection.addCustomFunction(self)
            self.intercept = aInterceptFunc

    
    def intercept(self, dbcursor, sql, parameters):
        with self.workinglock:
            ret = self.intercept(dbcursor, sql, parameters)
        return ret
        
class IntervalTimer:
    
    def __init__(self, aInterval, aTarget):
        self.stopflag = threading.Event()
        self.stopflag.clear()
        self.workerthread = None
        self.target = aTarget
        self.interval = aInterval
        
    def start(self):
        if self.workerthread == None:
            self.workerthread = threading.Thread(target=self.__worker)
            self.workerthread.start()
        
    def __worker(self):
        while True:
            if self.stopflag.wait(timeout=self.interval) == True:
                break
            else:
                self.target()
        
    def cancel(self):
        self.stopflag.set()
        if self.workerthread != None:
            self.workerthread.join()
        self.workerthread = None
    
    def __del__(self):
        self.stopflag.set()
        if self.workerthread != None:
            self.workerthread.join()
        self.workerthread = None
     
class MyConfig(newspaper.configuration.ArticleConfiguration):
    def __init__(self): 
        super().__init__()
        self.MIN_WORD_COUNT = 150  # num of word tokens in text
        self.MIN_SENT_COUNT = 4    # num of sentence tokens
        self.MAX_TITLE = 200       # num of chars
        self.MAX_TEXT = 1000000     # num of chars
        self.MAX_KEYWORDS = 35     # num of strings in list
        self.MAX_AUTHORS = 10      # num strings in list
        self.MAX_SUMMARY = 5000    # num of chars

        # Cache and save articles run after run
        self.memoize_articles = False

        # Set this to false if you don't care about getting images
        self.fetch_images = False
        self.image_dimension_ration = 16/9.0

