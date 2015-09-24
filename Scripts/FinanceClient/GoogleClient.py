from FinanceClient import FinanceClient
from datetime import date

class GoogleClient(FinanceClient):
    __clientName = 'GOOGLE'
    
    def getID(self):
        """Get the specific id for some client (usually the name of the polling service)"""
        return self.__clientName
    
    def getDateRangeData(self, aTicker, aStartDate, aEndDate):
        """Get the year to date financial data for the specified company ticker"""
        if not isinstance(aStartDate, date) :
            raise TypeError("aStartDate must be a date")
        if not isinstance(aEndDate, date) :
            raise TypeError("aEndDate must be a date")
        
        
