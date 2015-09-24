from FinanceClient import FinanceClient
from datetime import date
from urllib import request
from bs4 import BeautifulSoup as BS
from FinanceData import FinanceData
from builtins import str

class YahooClient(FinanceClient):
    __clientName = 'YAHOO'
    def getID(self):
        """
        Return the ID for this specific client (usually the name of the polling service)
        """
        
        return self.__clientName
    
    def getDateRangeData(self, aTicker, aStartDate, aEndDate):
        """
        Return the finance data for the specified company ticker
        """
        if not isinstance(aStartDate, date) :
            raise TypeError("aStartDate must be a date")
        if not isinstance(aEndDate, date) :
            raise TypeError("aEndDate must be a date")
        
        url = self.__requestBuilder(aTicker, aStartDate, aEndDate)
        print(url)
        req = request.urlopen(url)
        soup = BS(req, 'html.parser')
        fdata = self.__CSVtoFinanceData("".join(str(item) for item in soup.contents))
        return fdata
        
    def __requestBuilder(self, aTicker, aStartDate, aEndDate):
        """
        
        """
        url = 'http://ichart.yahoo.com/table.csv'
        url += '?s=' + aTicker
        
        url += '&a=' + str(aStartDate.month - 1)
        url += '&b=' + str(aStartDate.day)
        url += '&c=' + str(aStartDate.year)
        
        url += '&d=' + str(aEndDate.month - 1)
        url += '&e=' + str(aEndDate.day)
        url += '&f=' + str(aEndDate.year)
        
        url += '&g=' + 'd' #get at a daily interval
        url += '&ignore=.csv' #unsure of what this does but it seems to be required
        return url
    
    def __CSVtoFinanceData(self, aCSVData):
        fdata = FinanceData()
        lineiter = iter(aCSVData.splitlines())
        for line in lineiter:
            if not 'Date' in line: 
                comp = line.split(',')
                if not len(comp) == 7:
                    print("something odd happened")
                datecomp = comp[0].split('-')
                d = date.today().replace(day=int(datecomp[2]),month=int(datecomp[1]),year=int(datecomp[0]))
                fdata.insertPoint(d, comp[1], comp[2], comp[3], comp[4], comp[5])
                
        return fdata
    
    