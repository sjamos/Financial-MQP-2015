'''
Created on Nov 6, 2015

@author: Sean
'''
import BoWIDFI
import csv
import sqlite3
import IDFinanceClientIntelligent
import Logger
import Helpers
import cProfile

module_logger = Logger.getLogger(__name__)

def cleanBoWIDFDB(aDatabaseName):
    module_logger.info("Cleaning working DB" )
    
    databaseconnection = sqlite3.connect(aDatabaseName)
    cursor = databaseconnection.cursor()
    cursor.execute("UPDATE Articles SET Prediction=?, Actual=?, PCount=?, LCount=?;", (None, None, None, None))
    cursor.execute("DELETE FROM Words;")
    cursor.execute("DELETE FROM ToLearnQueue;")
    cursor.execute("DELETE FROM CurrentPredictions;")
    databaseconnection.commit()
    databaseconnection.close()
    
def addValues(aBoWIDFDBname, Start, Stop, doClean=True):
    module_logger.info("\n\n\n\n\n\n\n\n\n\n\n\n")
    module_logger.info("Starting Main Function")

    module_logger.info("Opening company information")
    
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
    
    if doClean:
        cleanBoWIDFDB(aBoWIDFDBname)
    
    module_logger.info("Creating Intraday client")
    idfci = IDFinanceClientIntelligent.IDFinanceClientIntelligentChooseAverage(sp500information, 
                                                                               Helpers.TSSQLConnection("../NewsData/IntradayData.db"), 
                                                                               leftMost=2, 
                                                                               rightMost=2)
    module_logger.info("Creating Intelligent agent")
    bowidfi = BoWIDFI.BoWIDFIntelligence(sp500information, aBoWIDFDBname, idfci)

    module_logger.info("Starting to add to queue")
    databaseconnection = sqlite3.connect("../NewsData/News.db")
    databaseconnection.row_factory = sqlite3.Row
    cursor = databaseconnection.cursor()
    limit = Stop
    blockpos = Start
    blocksize = 2500
    newsitems = 1
    
    while newsitems > 0 and blockpos < limit:
        newsitems = 0
        cursor.execute("SELECT * FROM News LIMIT ? OFFSET ?", (blocksize, blockpos))
        
        module_logger.info("Starting entries {0} - {1}".format(blockpos, blockpos + blocksize) )
        for item in cursor:
            bowidfi.learn(item["service"], item["ticker"], item["title"], item["pubdate"], item["link"], item["newsid"], item["fullsource"] )
            newsitems += 1
            if (newsitems + blockpos) > limit:
                break;
        blockpos += blocksize
     
    module_logger.info("Finished adding to intelligent client" )
    databaseconnection.close()
     
    module_logger.info("Closing workers" )
    bowidfi.close()
    idfci.close()
    module_logger.info("Finished" )

if __name__ == '__main__':
    #cProfile.run('addValues("../NewsData/BoW_IDF_Learner4.db", 73500, 00)', sort='tottime')
    addValues("../NewsData/BoW_IDF_Learner5.db", 65000, 1000000, True)