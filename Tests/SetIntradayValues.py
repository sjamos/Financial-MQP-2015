'''
Created on Oct 26, 2015

@author: Sean
'''
from IDFinanceSQLWriter import IDFinanceSQLWriter
from IDFinanaceClient import YahooIDFinanceClient,GoogleIDFinanceClient
import csv

if __name__ == '__main__':
    writer = IDFinanceSQLWriter("../NewsData/IntradayData.db")
    
    sp500file = open("../SP5002.csv", 'r')
    sp500information = {}
    reader = csv.reader(sp500file, dialect='excel')
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
    
    yidfc = YahooIDFinanceClient(writer)
    gidfc = GoogleIDFinanceClient(writer)
    yidfc.getData(sp500information)
    gidfc.getData(sp500information)
    
    writer.close()