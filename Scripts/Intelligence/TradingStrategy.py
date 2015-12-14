'''
Created on Nov 27, 2015

@author: Sean
'''

import numpy
import itertools
from multiprocessing import pool
import Helpers
import random
import math
import Logger
from sklearn.cross_validation import train_test_split

module_logger = Logger.getLogger(__name__)

def intervals(intervalStart, intervalEnd, intervalCount, includeNone=False):
    ret = []
    if includeNone:
        ret.append(None)
    increment =  (intervalEnd - intervalStart) / intervalCount
    for i in range(intervalCount+1):
        ret.append(i*increment+intervalStart)
        
    return ret

def makeOpinionReturn(volatilityTrend, startTrend, middleTrend, endTrend, overallTrend, predictionDiff,
                      volatitliyThresholds, startThresholds, middleTresholds, endThresholds, overallThresholds, predictionThresholds,
                      time=None):
    ret = []
    for vt, st, mt, et, ot, pt in itertools.product(volatitliyThresholds, startThresholds, middleTresholds, endThresholds, overallThresholds, predictionThresholds):
        val = {"ShouldBuy":True}
        if vt != None:
            val["ShouldBuy"] = val["ShouldBuy"] and volatilityTrend < vt
        val["volatility"] = vt
            
        if st != None:
            val["ShouldBuy"] = val["ShouldBuy"] and startTrend > st
        val["startTrend"] = st
            
        if mt != None:
            val["ShouldBuy"] = val["ShouldBuy"] and middleTrend > mt
        val["middleTrend"] = mt
            
        if et != None:
            val["ShouldBuy"] = val["ShouldBuy"] and endTrend > et
        val["endTrend"] = et
            
        if ot != None:
            val["ShouldBuy"] = val["ShouldBuy"] and overallTrend > ot
        val["overallTrend"] = ot
        
        if pt != None:
            val["ShouldBuy"] = val["ShouldBuy"] and predictionDiff > pt
        val["predictionValue"] = pt
        
        if time != None:
            val["time"] = time
            
        ret.append(val)
    return ret

def findClosest(aPredictionTV, value):
    closest = -100000
    for key in aPredictionTV.keys():
        if abs(value-closest) > abs(value-key):
            closest = key
    return closest
    
def getXYs(aPredictionTV, start, end, startinclusive = True, endinclusive = True):
    retx = []
    rety = []
    gcd = aPredictionTV.GCD()
    for key in aPredictionTV:
        if ((key > start) or (startinclusive and key == start)) and ((key < end) or (endinclusive and key == end)):
            retx.append(int(key/gcd))
            rety.append(aPredictionTV[key])
            
    return retx, rety

def calcPredictionDetails(aPredictionTV, Time, TimeBar):
    q0 = findClosest(aPredictionTV, Time)
    q1 = findClosest(aPredictionTV, Time + TimeBar/4.0*1.0)
    q2 = findClosest(aPredictionTV, Time + TimeBar/4.0*2.0)
    q3 = findClosest(aPredictionTV, Time + TimeBar/4.0*3.0)
    q4 = findClosest(aPredictionTV, Time + TimeBar/4.0*4.0)
            
    shortx, shorty = getXYs(aPredictionTV, q0, q2)
    middlex, middley = getXYs(aPredictionTV, q1, q3)
    tallx, tally = getXYs(aPredictionTV, q2, q4)
    allx, ally = getXYs(aPredictionTV, q0, q4)
    
    shortpoly = numpy.polyfit(shortx, shorty, 1)
    middlepoly = numpy.polyfit(middlex, middley, 1)
    tallpoly = numpy.polyfit(tallx, tally, 1)
    allpoly = numpy.polyfit(allx, ally, 1)
    
    diff = aPredictionTV[q4] - aPredictionTV[q0]
    return {"time":Time, "volatility":numpy.std(ally),"startTrend":shortpoly[0],"middleTrend":middlepoly[0],"endTrend":tallpoly[0],"overallTrend":allpoly[0], "diff":diff}
    
class stockOpinion:
    examplereturn = { "shouldBuy":False,
                      "volatility":0,
                      "startTrend":0,
                      "middleTrend":0,
                      "endTrend":0,
                      "overallTrend":0}
    
    mypool = None
    
    def __init__(self, aPrediction, start=0, timebar=(4*60*60)):
        self.__predictionTV = aPrediction
        self.__starttime = start
        self.timebar = timebar
        
        self.shouldBuy = None
        self.volatility = None
        self.startTrend = None
        self.middleTrend = None
        self.endTrend = None
        self.overallTrend = None
        self.__recalculate()
        
        self.startPool()
    
    def setBar(self, timebar=(4*60*60)):
        self.timebar = timebar
        self.__recalculate()
    
    def setStart(self, start=0):
        self.__starttime = start
        self.__recalculate()
    
    def startPool(self):
        if not stockOpinion.mypool:
            stockOpinion.mypool = pool.Pool()
        
    def __recalculate(self):
        adict = calcPredictionDetails(self.__predictionTV, self.__starttime, self.timebar)
        
        self.volatility = adict["volatility"]
        self.startTrend = adict["startTrend"]
        self.middleTrend = adict["middleTrend"]
        self.endTrend = adict["endTrend"]
        self.overallTrend = adict["overallTrend"]
        
    
    def findClosest(self, value):
        return findClosest(self.__predictionTV, value)
    
    def __getXYs(self, start, end, startinclusive = True, endinclusive = True):
        retx = []
        rety = []
        gcd = self.__predictionTV.GCD()
        for key in self.__predictionTV:
            if ((not startinclusive and key > start) or (startinclusive and key >= start)) and ((not endinclusive and key < end) or (endinclusive and key <= end)):
                retx.append(key/gcd)
                rety.append(self.__predictionTV[key])
                
        return retx, rety
    
    def opinion(self, vt=None, st=None, mt=None, et=None, ot=None, pt=None):
        diff = self.__predictionTV[self.findClosest(self.__starttime + self.timebar/4.0*4.0)] - self.__predictionTV[self.findClosest(self.__starttime)]
        return makeOpinionReturn(self.volatility, self.startTrend, self.middleTrend, self.endTrend, self.overallTrend, diff, 
                                   [vt], [st], [mt], [et], [ot], [pt])[0]
        
    def multiOpinion(self, vTs=[None], sTs=[None], mTs=[None], eTs=[None], oTs=[None], pTs=[None]):
        
        poolarguments = []

        diff = self.__predictionTV[self.findClosest(self.__starttime + self.timebar/4.0*4.0)] - self.__predictionTV[self.findClosest(self.__starttime)]
        for pt in pTs:
            poolarguments.append((self.volatility, self.startTrend, self.middleTrend, self.endTrend, self.overallTrend, diff,
                                  vTs, sTs, mTs, eTs, oTs, [pt]))
        
        
        layeredvalues = stockOpinion.mypool.starmap(makeOpinionReturn, poolarguments)
        
        flattenedvalues = [val for sublist in layeredvalues for val in sublist]
        
        return flattenedvalues
    
    def closePool(self):
        if stockOpinion.mypool:
            stockOpinion.mypool.close()
            stockOpinion.mypool.join()
            stockOpinion.mypool = None

def stockOpintionMultiOpinion(aPool, aPredictionTV, TimeBar, Times=[0], vTs=[None], sTs=[None], mTs=[None], eTs=[None], oTs=[None], pTs=[None]):
        
        poolarguments = []
        
        for time in Times:
            poolarguments.append((aPredictionTV, time, TimeBar))
        
        values = aPool.starmap(calcPredictionDetails, poolarguments)
        
        poolarguments = []
        for value in values:
            diff = aPredictionTV[findClosest(aPredictionTV, value["time"] + TimeBar/4.0*4.0)] - aPredictionTV[findClosest(aPredictionTV, value["time"])]
            for pt in pTs:
                poolarguments.append((value["volatility"], value["startTrend"], value["middleTrend"], value["endTrend"], value["overallTrend"], diff,
                                       vTs, sTs, mTs, eTs, oTs, [pt], value["time"]))
        
        layeredvalues = aPool.starmap(makeOpinionReturn, poolarguments)
        
        flattenedvalues = [val for sublist in layeredvalues for val in sublist]
        
        return flattenedvalues        






def calcPredictionAndActualDetails(aNewsID, aPredictionTV, aActualTV, Time, TimeBar):
    q0 = findClosest(aPredictionTV, Time)
    q1 = findClosest(aPredictionTV, Time + TimeBar/4.0*1.0)
    q2 = findClosest(aPredictionTV, Time + TimeBar/4.0*2.0)
    q3 = findClosest(aPredictionTV, Time + TimeBar/4.0*3.0)
    q4 = findClosest(aPredictionTV, Time + TimeBar/4.0*4.0)
            
    shortx, shorty = getXYs(aPredictionTV, q0, q2)
    middlex, middley = getXYs(aPredictionTV, q1, q3)
    tallx, tally = getXYs(aPredictionTV, q2, q4)
    allx, ally = getXYs(aPredictionTV, q0, q4)
    
    shortpoly = numpy.polyfit(shortx, shorty, 1)
    middlepoly = numpy.polyfit(middlex, middley, 1)
    tallpoly = numpy.polyfit(tallx, tally, 1)
    allpoly = numpy.polyfit(allx, ally, 1)
    
    pdiff = aPredictionTV[q4] - aPredictionTV[q0]
    adiff = aActualTV[q4] - aActualTV[q0]
    return {"NewsID":aNewsID, "time":Time, "volatility":numpy.std(ally),"startTrend":shortpoly[0],"middleTrend":middlepoly[0],"endTrend":tallpoly[0],"overallTrend":allpoly[0], "pDiff":pdiff, "aDiff":adiff }

class PersistentTradingPoints:
    maintable = "Main"
    columns = ["time", "volatility", "startTrend", "middleTrend", "endTrend", "overallTrend", "pDiff"]
    def __init__(self, apppdbname):
        self.dbconnection = Helpers.SQLConnection(apppdbname)
        self.dbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                        NewsID text,
                                        LCount int,
                                        time int,
                                        volatility float,
                                        startTrend float,
                                        middleTrend float,
                                        endTrend float,
                                        overallTrend float,
                                        pDiff float,
                                        aDiff float)""".format(self.maintable))
        self.workpool = pool.Pool()
        
    def clean(self):
        self.dbconnection.execute("DELETE FROM {0}".format(self.maintable))
        
    def getData(self, Columns=None, Transpose=False):
        module_logger.info("Getting data")
        if not Columns:
            Columns = self.columns
        xdata = self.dbconnection.execute("SELECT {0} FROM {1}".format(",".join(Columns), self.maintable)) 
        ydata = [val[0] for val in self.dbconnection.execute("SELECT aDiff FROM {0}".format(self.maintable))]
        
        if Transpose:
            xdata = list(map(list, zip(*xdata)))
        module_logger.info("Finished getting data")
        return xdata, ydata
    
    def getRandomData(self, usablePercent=None, selectedPercent = .3, Columns=None, Transpose=False):
        module_logger.info("Getting random data")
        rand = random.SystemRandom()
        
        xdata, ydata = self.getData(Columns, Transpose=False)
        
        if usablePercent:
            xdata, tempx, ydata, tempy = train_test_split(xdata, ydata, test_size=usablePercent, random_state=rand.randint(0, 1000))
        
        trainx, testx, trainy, testy = train_test_split(xdata, ydata, test_size=selectedPercent, random_state=rand.randint(0, 1000))
        
        if Transpose:
            trainx = list(map(list, zip(*trainx)))
            testx = list(map(list, zip(*testx)))
        
        return trainx, testx, trainy, testy
            
        
    def addPoint(self, newsid, LCount, prediction, actual, times=[0], timebar=4*60*60):
        poolarguments = []
        self.dbconnection.execute("DELETE FROM {0} WHERE LCount NOT IN ( ? )".format(self.maintable), (LCount,))
        
        for time in times:
            if not self.dbconnection.execute("SELECT * FROM {0} WHERE NewsID=? and time=?".format(self.maintable), (newsid, time)):
                poolarguments.append((newsid, prediction, actual, time, timebar))
        
        values = self.workpool.starmap(calcPredictionAndActualDetails, poolarguments)
        
        for value in values:
            self.dbconnection.execute('INSERT INTO {0} VALUES (?,?,?,?,?,?,?,?,?,?)'.format(self.maintable), 
                                        (value["NewsID"], LCount, value["time"], value["volatility"],
                                         value["startTrend"], value["middleTrend"], value["endTrend"],
                                         value["overallTrend"], value["pDiff"], value["aDiff"]))
           
    def addBulkPoints(self, aNewsIDList, LCount, PredictionList, ActualList, Times=[0], timebar=4*60*60):
        poolarguments = []
        self.dbconnection.execute("DELETE FROM {0} WHERE LCount NOT IN ( ? )".format(self.maintable), (LCount,))
        module_logger.info("Starting bulk add for {0}".format(LCount) )
        for i in range(len(aNewsIDList)):
            for time in Times:
                if not self.dbconnection.execute("SELECT * FROM {0} WHERE NewsID=? and time=?".format(self.maintable), (aNewsIDList[i], time, )):
                    poolarguments.append((aNewsIDList[i], PredictionList[i], ActualList[i], time, timebar))
        module_logger.info("Finished creating pool arguments")         
        blocksize = 50000.0
        for i in range(math.ceil(len(poolarguments) / blocksize)):
            module_logger.info("Starting Bulk add {0}-{1}".format(int(blocksize*i), int(blocksize*(i+1))) )
            values = self.workpool.starmap(calcPredictionAndActualDetails, poolarguments[int(blocksize*i) : int(blocksize*(i+1))])
            for value in values:
                self.dbconnection.execute('INSERT INTO {0} VALUES (?,?,?,?,?,?,?,?,?,?)'.format(self.maintable), 
                                            (value["NewsID"], LCount, value["time"], value["volatility"],
                                             value["startTrend"], value["middleTrend"], value["endTrend"],
                                             value["overallTrend"], value["pDiff"], value["aDiff"]))
            
        
         
    def __del__(self):
        self.dbconnection.close()
        
        

        