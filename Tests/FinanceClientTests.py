import unittest
from FinanceClientFactory import FinanceClientFactory
from datetime import date
from FinanceData import FinanceData

class YahooClientTest(unittest.TestCase):
    __client = None
    
    def setUp(self):
        self.__client = FinanceClientFactory().getFinanceClient('YAHOO')
        
    def tearDown(self):
        self.__client = None

    def test_GoogleJan2nd2015(self):
        expecteddata = FinanceData()
        
        expecteddata.insertPoint(date(2015, 1, 2), 529.012399, 531.272443, 524.102327, 524.812404, 1447600)
        
        startandenddate = date(2015, 1, 2)
        actualdata = self.__client.getDateRangeData('GOOG', startandenddate, startandenddate)
        
        self.assertFinanceDataEqual(expecteddata, actualdata)
        
    def assertFinanceDataEqual(self, ExpectedData, ActualData):
        exdata = ExpectedData.datapoints[:]
        acdata = []
        for acpoint in ActualData.datapoints:
            exrem = None
            for expoint in exdata:
                if expoint == acpoint:
                    exrem = expoint
                    break
            if exrem is None:
                acdata.append(acpoint)
            else: 
                exdata.remove(exrem)
        
        failstr = '' 
        if len(exdata) > 0:
            failstr += "\nNon-Matched Expected Data:" + " \n".join(str(item) for item in exdata) + "\n"
            
        if len(acdata) > 0:
            failstr += "\nNon-Matched Actual Data:" + " \n".join(str(item) for item in acdata) + "\n"
            
        self.assertTrue(len(failstr) == 0, failstr)
    

def suite():
    suite = unittest.TestSuite()
    suite.addTest(YahooClientTest())
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    test_suite = suite()
    runner.run(test_suite)