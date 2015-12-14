'''
Created on Oct 1, 2015

@author: Sean
'''
from FinanceNewsClientFactory import FinanceNewsClientFactory
import csv
from FinanceNewsClientWriter import FinanceNewsSQLWriter
    
if __name__ == '__main__':
    sp500file = open("../SP5002.csv", 'r')
    sp500information = {}
    reader = csv.reader(sp500file, dialect='excel')
    
    fncf = FinanceNewsClientFactory()
    yfnc = fncf.getFinanceClient("YAHOO")
    gfnc = fncf.getFinanceClient("GOOGLE")
    
    for row in reader:
        if "Ticker" not in row:
            toadd = {}
            toadd["name"] = row[2]
            toadd["exchange"] = row[1]
            toadd["sector"] = row[3]
            toadd["subsector"] = row[4]
            toadd['news'] = {}
            sp500information[row[0]] = toadd
    
    sp500file.close()
    filewriter = FinanceNewsSQLWriter("../NewsData/News.db",sp500information)
    gfnc.getMultipleNews(sp500information, filewriter)
    yfnc.getMultipleNews(sp500information, filewriter)
    filewriter.close()
    
    
        
    
        
    
    
    