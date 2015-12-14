import FinanceNewsClient


class FinanceNewsClientFactory:
    __financenewsclients = {}
    
    def __init__(self):
        self.registerClient(FinanceNewsClient.GoogleNewsClient())
        self.registerClient(FinanceNewsClient.YahooNewsClient())
    
    def registerClient(aFinanceNewsClient):
        """Register a specific client for this factory"""
        if not isinstance(aFinanceNewsClient, FinanceNewsClient.FinanceNewsClient) :
            raise TypeError("afinanceClient must be a FinanceClient")
        aid = aFinanceNewsClient.getID()
        if aid in FinanceNewsClientFactory.__financenewsclients:
            return False
        FinanceNewsClientFactory.__financenewsclients[aid] = aFinanceNewsClient
        return True
    registerClient = staticmethod(registerClient)
 
    def getFinanceClient(aID):
        """Get a financeClient based on its id"""
        if aID not in FinanceNewsClientFactory.__financenewsclients:
            raise TypeError(aID +" is not a recognized client")
        return FinanceNewsClientFactory.__financenewsclients[aID]
    getFinanceClient = staticmethod(getFinanceClient)