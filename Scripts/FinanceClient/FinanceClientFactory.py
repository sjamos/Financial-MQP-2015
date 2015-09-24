from FinanceClient import FinanceClient
from GoogleClient import GoogleClient
from YahooClient import YahooClient

class FinanceClientFactory:
    __financeclients = {}
    
    def __init__(self):
        self.registerClient(GoogleClient())
        self.registerClient(YahooClient())
    
    def registerClient(aFinanceClient):
        """Register a specific client for this factory"""
        if not isinstance(aFinanceClient, FinanceClient) :
            raise TypeError("afinanceClient must be a FinanceClient")
        aid = aFinanceClient.getID()
        if aid in FinanceClientFactory.__financeclients:
            return False
        FinanceClientFactory.__financeclients[aid] = aFinanceClient
    registerClient = staticmethod(registerClient)
 
    def getFinanceClient(aID):
        """Get a financeClient based on its id"""
        if aID not in FinanceClientFactory.__financeclients:
            raise TypeError(aID +" is not a recognized client")
        return FinanceClientFactory.__financeclients[aID]
    getFinanceClient = staticmethod(getFinanceClient)