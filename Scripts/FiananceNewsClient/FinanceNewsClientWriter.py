import os
import pickle
import json
import sqlite3
from datetime import datetime
from pytz import UTC

class FinanceNewsPickleAndJSONWriter:
    def __init__(self, companyData):
        self.completedata = companyData
    
    def addNewsItem(self, aNewsItem):
        if(aNewsItem.isSuccessful()):
            ticker = aNewsItem.fullfeed.ticker
            service = aNewsItem.service
            toadd = {}
            toadd["source"] = aNewsItem.fullsource
            toadd["title"] = aNewsItem.feedentry.title
            toadd["desc"] = aNewsItem.feedentry.description
            toadd["pub"] = aNewsItem.feedentry.published
            toadd["ppub"] = aNewsItem.feedentry.published_parsed
            toadd["id"] = aNewsItem.feedentry.id
            toadd["link"] = aNewsItem.url
            
            if(ticker in self.completedata):
                if("News" not in self.completedata[ticker]):
                    self.completedata[ticker]["News"] = {}
                if(service not in self.completedata[ticker]["News"]):
                    self.completedata[ticker]["News"][service] = []
                self.completedata[ticker]["News"][service].append(toadd) 
     
    def doesNewsItemExist(self, aRSSNewsEntry):
        return False
           
    def close(self):
        newfile = self.getNewFile('.p', "wb")
        pickle.dump(self.completedata, newfile )
        newfile.close()
        
        newfile2 = self.getNewFile('.json', "w")
        json.dump(self.completedata, newfile2, indent=4, separators=(',', ': '))
        newfile2.close()
    
    def getNewFile(self, extension='.txt', filetype="w"):
        i = 1
        while(True):
            fname = "../NewsData/NewsDump" + str(i) + extension
            if not os.path.isfile(fname):
                return open(fname, filetype)
            i += 1

class FinanceNewsSQLWriter:
    company_table = "Companies"
    news_table = "News"
    news_short_table = "Newspaper"
    
    def __init__(self, aDatabaseName, aCompanyInformation):
        self.databaseconnection = sqlite3.connect(aDatabaseName)
        self.cursor = self.databaseconnection.cursor()
        self.__fillCompanyTable(aCompanyInformation)
        
    def __fillCompanyTable(self, aCompanyInformation):
        if(self.doesTableExist(self.company_table)):
            self.cursor.execute("CREATE TABLE ? (name text, exchange text, ticker text, sector text, subsector text)",(self.company_table))
            self.databaseconnection.commit()
  
        for company in aCompanyInformation:
            self.cursor.execute("SELECT * FROM Companies WHERE ticker=?",(company,))
            if(self.cursor.fetchone() == None):
                self.cursor.execute("INSERT INTO Companies VALUES (?,?,?,?,?)", (aCompanyInformation[company]["name"],
                                                                         aCompanyInformation[company]["exchange"], 
                                                                         company,
                                                                         aCompanyInformation[company]["sector"],
                                                                         aCompanyInformation[company]["subsector"],))  
                self.databaseconnection.commit()
        
    def addNewsItem(self, aNewsItem):
        self.checkNewsTable()
        if(aNewsItem.isSuccessful()):
            ticker = aNewsItem.fullfeed.ticker
            service = aNewsItem.service
            fullsource = aNewsItem.fullsource
            title = aNewsItem.feedentry.title
            desc = aNewsItem.feedentry.description
            pub = UTC.localize(datetime.strptime(aNewsItem.feedentry.published, "%a, %d %b %Y %H:%M:%S %Z")).isoformat()
            newsid = aNewsItem.feedentry.id
            link = aNewsItem.url
            self.cursor.execute("INSERT INTO News VALUES (?,?,?,?,?,?,?,?)", (ticker, service, title, pub, desc, link, newsid, fullsource,))
            self.databaseconnection.commit()


    def doesNewsItemExist(self, aRSSNewsEntry):
        self.checkNewsTable()
        self.cursor.execute("SELECT * FROM News WHERE newsid=?",(aRSSNewsEntry.id,))
        if(self.cursor.fetchone() == None):
            return False
        return True
    
    def checkNewsTable(self):
        if(self.doesTableExist(self.news_table)):
            self.cursor.execute("CREATE TABLE News (ticker text, service text, title text, pubdate text, desc text, link text, newsid text, fullsource text)")
            self.databaseconnection.commit()
    
    def close(self):
        self.databaseconnection.close()
                
    def doesTableExist(self, aTableName):
        self.cursor.execute("SELECT * FROM sqlite_master WHERE name=? and type='table'",(aTableName,))
        return self.cursor.fetchone() == None
        
        
        
        