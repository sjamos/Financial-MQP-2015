import abc
import feedparser
from requests_futures.sessions import FuturesSession
import time
import requests
import sys
import socket

class FinanceNewsClient: 
    @abc.abstractmethod
    def getID(self):
        """Get the specific id for some client (usually the name of the polling service)"""
    getID = staticmethod(getID)
    
    @abc.abstractmethod
    def getNews(self, aTicker):
        """"""
    @abc.abstractmethod 
    def getMultipleNews(self, aTickerList):
        """"""
        
class RequestGeneral:
    def __init__(self, aUniqueKey, aURL, aSession, aStatusTracker, aService):
        self.uniquekey = aUniqueKey
        self.url = aURL
        self.mysession = aSession
        self.actsession = self.mysession.get(self.url, timeout=(5, 30))
        self.status = aStatusTracker
        self.status.changeValue(self.uniquekey, 'Queued')
        self.service = aService
        
    def isSuccessful(self):
        return self.status[self.uniquekey] == 'Success'
        
class RSSFeed(RequestGeneral):
    def __init__(self, aTicker, aURL,aSession, aRSSStatusTracker, aService):
        self.ticker = aTicker
        super().__init__(aTicker, aURL, aSession, aRSSStatusTracker, aService) 
        self.newsarticles = []

    def createNewsEntries(self, aNewsStatusTracker, aFinanceNewsWriter=None):
        if(not self.isSuccessful()):
            try:
                resp = self.actsession.result()
                if resp.status_code == 200:           
                    self.status.changeValue(self.uniquekey, 'Success')
                    self.fullfeed = feedparser.parse(resp.content)
                    for entry in self.fullfeed.entries:
                        if(aFinanceNewsWriter==None or not aFinanceNewsWriter.doesNewsItemExist(entry)):
                            self.newsarticles.append(NewsEntry(self, entry, self.mysession, aNewsStatusTracker, self.service))
                else:
                    self.status.changeValue(self.uniquekey, 'Error')
                    self.actsession = self.mysession.get(self.url, timeout=(5, 30))
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, socket.timeout):
                self.status.changeValue(self.uniquekey,  'Timeout')
                self.actsession = self.mysession.get(self.url, timeout=(5, 30))
            except:
                self.status.changeValue(self.uniquekey,  'Error')
                self.actsession = self.mysession.get(self.url, timeout=(5, 30))
                
        return self.newsarticles
        
class NewsEntry(RequestGeneral):
    def __init__(self, aRSSFeed, aFeedParserEntry, aSession, aNewsStatusTracker, aService):
        super().__init__(aNewsStatusTracker.getUniqueKey(), aFeedParserEntry.link, aSession, aNewsStatusTracker, aService)
        self.feedentry = aFeedParserEntry
        self.fullfeed = aRSSFeed
        self.fullsource = ''
        
    def completeRequest(self, aFinanceNewsWriter=None):
        if(not self.isSuccessful()):
            try:
                resp = self.actsession.result()
                if resp.status_code == 200:           
                    self.status.changeValue(self.uniquekey, 'Success')
                    self.fullsource = resp.text
                    if(aFinanceNewsWriter != None):
                        aFinanceNewsWriter.addNewsItem(self)
                        
                else:
                    self.status.changeValue(self.uniquekey, 'Error')
                    self.actsession = self.mysession.get(self.url, timeout=(5, 30))
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, socket.timeout):
                self.status.changeValue(self.uniquekey,  'Timeout')
                self.actsession = self.mysession.get(self.url, timeout=(5, 30))
            except (requests.exceptions.RequestException):
                self.status.changeValue(self.uniquekey,  'Error')
                self.actsession = self.mysession.get(self.url, timeout=(5, 30))
            except:
                self.status.changeValue(self.uniquekey,  'Error')
                self.actsession = self.mysession.get(self.url, timeout=(5, 30))
                
        return self.fullsource
    
class FinanceRSSNewsClient(FinanceNewsClient):
    class StatusTracker:
        statuses = None
        statustotals = None
        
        def __init__(self):
            self.statuses = {}
            self.statustotals = {'Timeout':0,'Error':0,'Success':0,'Queued':0}
        
        def changeValue(self, aKey, aNewState):
            if(aKey in self.statuses and self.statuses[aKey] in self.statustotals):
                self.statustotals[self.statuses[aKey]] -= 1                
            self.statuses[aKey] = aNewState
            self.statustotals[self.statuses[aKey]] += 1
            
        def getUniqueKey(self):
            attemptval = 0
            while(attemptval in self.statuses):
                attemptval+=1
            return attemptval
        
        def getPrintValue(self):
            return " Q:{0},S:{1},T:{2},E:{3} ".format(self.statustotals["Queued"], self.statustotals["Success"], self.statustotals["Timeout"], self.statustotals["Error"]);
        
        def doReset(self):
            for key in self.statuses:
                if self.statuses[key] != 'Success' and self.statuses[key] != 'Queued':
                    self.changeValue(key, 'Queued')
                    
        def isQueueEmpty(self):
            return self.statustotals['Timeout'] == 0 and self.statustotals['Error'] == 0 and self.statustotals['Queued'] == 0 
        
        def __getitem__(self, key):
            return self.statuses[key]
        
        def __format__(self, formatstr):
            return self.getPrintValue()

    totaltickers = 0
    rssstatus = None
    newsstatus = None
    currentstate = ''
    
    def __init__(self):
        self.totaltickers = 0
        self.rssstatus = self.StatusTracker()
        self.newsstatus = self.StatusTracker()
        self.currentstate = ''
        
    def printbars(self):
        pb = "\r{0}-{1} -> Total={2}: RSS -{3}: News -{4}".format(self.getID(),self.currentstate, self.totaltickers,
                                                                  self.rssstatus, self.newsstatus)
        sys.stdout.write(pb)
    
    def printSleepStatus(self, aSleepCount):
        print()
        for i in range(aSleepCount):
            time.sleep(1)
            ss = '\rSleep status: {:.2%}'.format(i/aSleepCount)
            sys.stdout.write(ss)
        ss = '\rSleep status: {:.2%}'.format(aSleepCount/aSleepCount)
        sys.stdout.write(ss)
        print()
        
    @abc.abstractmethod
    def getID(self):
        """Get the specific id for some client (usually the name of the polling service)"""

    def getNews(self, aTicker, aExchange, aFinanceWriter, AttemptsAtRSS=4, AttemptsAtNews=3, MaxSessionWorkers=10):
        """"""
        return self.getMultipleNews({aTicker:{'exchange':aExchange}}, aFinanceWriter, AttemptsAtRSS, AttemptsAtNews, MaxSessionWorkers)
    
    def getMultipleNews(self, aTickerList, aFinanceWriter, AttemptsAtRSS=4, AttemptsAtNews=3, MaxSessionWorkers=75):
        """"""
        print("\n------Running news from " + self.getID() + "------")
        session = FuturesSession(max_workers=MaxSessionWorkers)
        self.currentstate = "Initializing RSS"
        self.totaltickers = len(aTickerList)
        self.printbars()
        rssfeeds = []
        newsentries = []
        
        for ticker in aTickerList:
            url = self.getRSSURL(ticker, aTickerList[ticker]['exchange'])
            time.sleep(self.getRSSWait())
            rssfeeds.append(RSSFeed(ticker, url, session, self.rssstatus, self.getID()))
            self.printbars()
            
        for i in range(AttemptsAtRSS):         
            self.printSleepStatus(10)
            self.currentstate = "RSS Round " + str(i+1)
            self.rssstatus.doReset()
            for rssfeed in rssfeeds:
                newsentries.extend(rssfeed.createNewsEntries(self.newsstatus, aFinanceWriter))
                self.printbars()
            if(self.rssstatus.isQueueEmpty()):
                break
        
        for i in range(AttemptsAtNews):
            self.printSleepStatus(20)
            self.currentstate = "RSS News Round " + str(i+1)
            self.newsstatus.doReset()
            for newsentry in newsentries:
                newsentry.completeRequest(aFinanceWriter)
                self.printbars()
            if(self.newsstatus.isQueueEmpty()):
                break

        print("\nDONE")

    @abc.abstractmethod
    def getRSSURL(self, aTicker, aExchange):
        """"""  
        
    @abc.abstractmethod   
    def getRSSWait(self):
        """"""  
        
    @abc.abstractmethod   
    def getNewsWait(self):
        """"""  
    
class YahooNewsClient(FinanceRSSNewsClient):
    __clientName = 'YAHOO'
    def getID(self):
        """Get the specific id for some client (usually the name of the polling service)"""
        return self.__clientName
    
    def getRSSURL(self, aTicker, aExchange):
        return "http://finance.yahoo.com/rss/headline?s=" + aTicker.replace(".","-")
    
    def getRSSWait(self):
        """"""  
        return .1
    
    def getNewsWait(self):
        """"""  
        return 0.01
        
class GoogleNewsClient(FinanceRSSNewsClient):
    __clientName = 'GOOGLE'
    def getID(self):
        """Get the specific id for some client (usually the name of the polling service)"""
        return self.__clientName
        
    def getRSSURL(self, aTicker, aExchange):
        return "https://www.google.com/finance/company_news?q=" + aExchange + ":" + aTicker.replace("-",".") + "&output=rss"
    
    def getRSSWait(self):
        """"""  
        return .33
    
    def getNewsWait(self):
        """"""  
        return 0.025
        