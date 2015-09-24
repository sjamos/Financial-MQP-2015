import abc
import datetime
from builtins import int

class FinanceClient: 
    @abc.abstractmethod
    def getID(self):
        """Get the specific id for some client (usually the name of the polling service)"""
        return self.__clientName
    getID = staticmethod(getID)
    
    def getYTDData(self, aTicker):
        """Get the year to date financial data for the specified company ticker"""
        enddate = datetime.date.today()
        startdate = datetime.date.today().replace(day=1,month=1)
        return self.getDateRangeData(aTicker, startdate, enddate)
    
    def getPastDaysData(self, aTicker, aNumofDays):
        """Get the past given number of days of financial data (days include no trading days)"""
        if not isinstance(aNumofDays, int) :
            raise TypeError("aNumofDays must be a integer")
        enddate = datetime.date.today()
        startdate = datetime.date.today() - datetime.timedelta(day=aNumofDays)
        return self.getDateRangeData(aTicker, startdate, enddate)
    
    @abc.abstractmethod
    def getDateRangeData(self, aTicker, aStartDate, aEndDate):
        """Get the year to date financial data for the specified company ticker"""