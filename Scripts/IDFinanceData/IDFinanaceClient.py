'''
Created on Oct 26, 2015

@author: Sean
'''
import abc
from requests_futures.sessions import FuturesSession
from IDFinanceData import IDFinanceData

class IDFinanceClient(object):
    '''
    classdocs
    '''

    def __init__(self, aIDFinanceWriter=None):
        '''
        Constructor
        '''
        self.writer = aIDFinanceWriter
        
    def getData(self, aTickerExchangeDict):
        session = FuturesSession(max_workers=20)
        financedata = []
        for ticker in aTickerExchangeDict:
            url = self.getURL(ticker, aTickerExchangeDict[ticker]['exchange'])
            financedata.append(IDFinanceData(ticker, url, session, self.getID(), self.cleanData))
            
        for _ in range(3):
            dostop = True
            for fdp in financedata:
                dostop = dostop and (fdp.setData() != None)
            if(dostop):
                break
           
        if( self.writer != None and self.writer.write != None):
            self.writer.write(financedata)
        
        return financedata
    
    @abc.abstractmethod
    def getURL(self, aTicker, aExchange):
        """ Get the URL specific to an implementation """
        
    @abc.abstractmethod 
    def cleanData(self, aResponseContent):
        """ Remove the header """

    @abc.abstractmethod 
    def getID(self):
        """ Return the unique identifier for this intraday finance client """
    
    
class YahooIDFinanceClient(IDFinanceClient):
    def __init__(self, aIDFinanceWriter=None):
        super().__init__(aIDFinanceWriter)
    
    def getURL(self, aTicker, aExchange):
        """ Get the URL specific to an implementation """
        return "http://chartapi.finance.yahoo.com/instrument/1.0/{0}/chartdata;type=quote;range=15d/csv".format(aTicker)
        
    def cleanData(self, aResponseContent):
        """ Remove the header """
        ret = []
        for line in aResponseContent.splitlines():
            if (len(line) > 5 and ":" not in line):
                parts = line.strip().split(",")
                if (len(parts) == 6):
                    ret.append((int(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), int(parts[5])))
            elif( "message" in line):
                ret = []
                break
            
        if(len(ret) <= 0):
            ret = None
        return ret

    def getID(self):
        """ Return the unique identifier for this intraday finance client """
        return "YAHOO"
    
class GoogleIDFinanceClient(IDFinanceClient):
    def __init__(self, aIDFinanceWriter=None):
        super().__init__(aIDFinanceWriter)
    
    def getURL(self, aTicker, aExchange):
        """ Get the URL specific to an implementation """
        return "https://www.google.com/finance/getprices?q={0}&x={1}&i=60&p=15d&f=d,c,h,l,o,v".format(aTicker.replace("-","."), aExchange)
        
    def cleanData(self, aResponseContent):
        """ Remove the header """
        ret = []
        interval = 60
        lastfulltime = 0
        for line in aResponseContent.splitlines():
            if(len(line) > 5 and "exchange" not in line.lower() and "=" not in line):
                parts = line.strip().split(",")
                if (len(parts) == 6):
                    
                    currenttime = 0
                    if(parts[0][0] == 'a'):
                        currenttime = int(parts[0][1:])
                        lastfulltime = currenttime
                    else:
                        currenttime = lastfulltime + interval*int(parts[0])
                        
                    ret.append((int(currenttime), float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), int(parts[5])))
        if(len(ret) <= 0):
            ret = None
        return ret

    def getID(self):
        """ Return the unique identifier for this intraday finance client """
        return "GOOGLE"
