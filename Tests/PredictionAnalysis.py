'''
Created on Nov 19, 2015

@author: Sean
'''

import sqlite3
import ThreadedBoWIDFI
import random
import math
from TradingStrategy import intervals, stockOpinion, findClosest, stockOpintionMultiOpinion
from multiprocessing import pool
import Logger


module_logger = Logger.getLogger(__name__)

def singleValue(aDatabaseName):
    csvfile = open("../NewsData/TSO/TradingStrategy.csv", "w")    
    csvfile.write("PCount, LCount, Released, Opinion Given, Prediction Distance From Release, Predicted Value, Actual Value, Should Buy, Correct, Made Money,, Volatility, Overall Trend, Starting Trend, Middle Trend, Ending Trend\n" )
    
    acsvfile = open("../NewsData/TSO/Actuals.csv", "w")    
    acsvfile.write("Prediction Distance From Release, Volatility, Overall Trend, Starting Trend, Middle Trend, Ending Trend\n" )

    databaseconnection = sqlite3.connect(aDatabaseName)
    cursor = databaseconnection.cursor()
    
    limit = 1000000
    blockpos = 0
    blocksize = 2500
    newsitems = 1
    
    rand = random.SystemRandom()
    
    alldata = []
    
    while newsitems > 0 and blockpos < limit:
        newsitems = 0
        cursor.execute("SELECT * FROM Articles WHERE LCount>1000 LIMIT ? OFFSET ? ", (blocksize, blockpos))
        print("-- Starting entries {0} - {1} --".format(blockpos, blockpos + blocksize) )
        
        for articledetails in cursor:
            prediction = ThreadedBoWIDFI.ValueTimes.loads(articledetails[7])
            actual = ThreadedBoWIDFI.ValueTimes.loads(articledetails[8])
            if prediction != None and actual != None:
                pubdate = articledetails[3]
                opinion = stockOpinion(prediction)
                
                for _ in range(1):
                    time = opinion.findClosest(rand.randint(0, 10*60*60))
                    opinion.setStart(time)
                    opdict = opinion.opinion(st=None, mt=None, et=None, ot=0.005, pt=0.225)
                    shouldbuy = opdict["ShouldBuy"]
                    if shouldbuy:
                        shouldbuyval = 1
                    else:
                        shouldbuyval = 0
                        
                    actualchange = actual[time + opinion.timebar] - prediction[time]
                    if ((shouldbuy) and actualchange > 0.01) or ((not shouldbuy) and actualchange < -0.01):
                        correctval = 1
                    else:
                        correctval = 0
                        
                    
                        
                    if shouldbuy:
                        if actualchange > 0.01:
                            mademoney = 1
                            mademoneystr = "1"
                        else:
                            mademoney = 0
                            mademoneystr = "0"
                    else:
                        mademoney = 0
                        mademoneystr = ""
                        
                        
                        
                    csvfile.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9},, {10}, {11}, {12}, {13}, {14}\n".format(articledetails[9],
                                                                                    articledetails[10], 
                                                                                    pubdate,
                                                                                    pubdate + time, 
                                                                                    time + 4*60*60, 
                                                                                    prediction[time],
                                                                                    actual[time],
                                                                                    shouldbuyval,
                                                                                    correctval,
                                                                                    mademoneystr,
                                                                                    opinion.volatility,
                                                                                    opinion.overallTrend,
                                                                                    opinion.startTrend,
                                                                                    opinion.middleTrend,
                                                                                    opinion.endTrend))
                    
                    actualopinion = stockOpinion(actual, time)
                    acsvfile.write("{0},{1},{2},{3},{4},{5}\n".format(time + 4*60*60,
                                                                    actualopinion.volatility,
                                                                                    actualopinion.overallTrend,
                                                                                    actualopinion.startTrend,
                                                                                    actualopinion.middleTrend,
                                                                                    actualopinion.endTrend))
                    
                    alldata.append((articledetails[9], shouldbuyval, correctval, mademoney))
                    
            newsitems += 1
        blockpos += blocksize
    
    databaseconnection.close()
    csvfile.close()
    acsvfile.close()
    
    csvfileaverage = open("../NewsData/TSO/TradingStrategyAverage.csv", "w")
    
    averagesize = 200.0
    alldata.sort(key=lambda a: a[0])
    
    if (sum([a[1] for a in alldata]) != 0):
        csvfileaverage.write(",,{0},{1},{2}\n".format( sum([x[1] for x in alldata])/len(alldata),
                                                        sum([x[2] for x in alldata])/len(alldata),
                                                        sum([x[3] for x in alldata])/sum([a[1] for a in alldata])))
    else:
        csvfileaverage.write("{0},{1},{2},{3},{4}\n".format(sum([x[1] for x in alldata])/len(alldata),
                                                            sum([x[2] for x in alldata])/len(alldata),
                                                            "inf"))

    csvfileaverage.write("PCount-Start, PCount-End, Should Buy %, Correct %, Made Money %\n" )
    
    for i in range(math.floor(len(alldata)/averagesize)-1):
        temp = alldata[int(averagesize*i):int(averagesize*(i+1))]
        if (sum(a[1] for a in temp) != 0):
            csvfileaverage.write("{0},{1},{2},{3},{4} \n".format(temp[0][0], temp[-1][0], sum(a[1] for a in temp)/len(temp), sum(a[2] for a in temp)/len(temp), sum(a[3] for a in temp)/sum(a[1] for a in temp)))
        else:
            csvfileaverage.write("{0},{1},{2},{3},{4} \n".format(temp[0][0], temp[-1][0], sum(a[1] for a in temp)/len(temp), sum(a[2] for a in temp)/len(temp), "inf"))
    
    csvfileaverage.close()

def mutliValue(aDatabaseName):
    module_logger.info("\n\n\n\n\n\n\n\n\n\n\n\n")
    
    ets = intervals(-0.005, 0.01, 30, includeNone=True)
    ots = intervals(-0.005, 0.01, 30, includeNone=True)
    vts = [None]#intervals(.1, 1, 10, includeNone=True)
    tts = intervals(0, 12*60*60, 12)
    pts = intervals(0, 0.15, 7, includeNone=True)
    
    alldata = {}
#    fulldata = []
    
    for pt in pts:
        alldata[pt] = {}        
        for tt in tts:
            alldata[pt][tt] = {}
            for vt in vts:
                alldata[pt][tt][vt] = {}
                for ot in ots:
                    alldata[pt][tt][vt][ot] = {}
                    for et in ets:
                        alldata[pt][tt][vt][ot][et] = {"Total":0, "ShouldBuy":0, "MadeMoney":0, "Correct":0}
                        
    
    databaseconnection = sqlite3.connect(aDatabaseName)
    cursor = databaseconnection.cursor()
        
    limit = 1000000
    blockpos = 0
    blocksize = 500
    newsitems = 1
    
    while newsitems > 0 and blockpos < limit:
        newsitems = 0
        cursor.execute("SELECT * FROM Articles WHERE LCount>500 LIMIT ? OFFSET ? ", (blocksize, blockpos))
        module_logger.info("-- Starting entries {0} - {1} --".format(blockpos, blockpos + blocksize) )
        
        for articledetails in cursor:
            prediction = ThreadedBoWIDFI.ValueTimes.loads(articledetails[7])
            actual = ThreadedBoWIDFI.ValueTimes.loads(articledetails[8])
            if prediction != None and actual != None:
                
                opinion = stockOpinion(prediction)                
                
                for atime in tts:
                    time = opinion.findClosest(atime)
                    opinion.setStart(time)
                    opinions = opinion.multiOpinion(eTs=ets, oTs=ots, vTs=vts, pTs=pts)
                    
                    actualchange = actual[time + opinion.timebar] - actual[time]
                    for op in opinions:
                        alldata[op["predictionValue"]][atime][op["volatility"]][op["overallTrend"]][op["endTrend"]]["Total"] += 1
                        if op["ShouldBuy"]:
                            alldata[op["predictionValue"]][atime][op["volatility"]][op["overallTrend"]][op["endTrend"]]["ShouldBuy"] += 1
                            if actualchange > 0.00:
                                alldata[op["predictionValue"]][atime][op["volatility"]][op["overallTrend"]][op["endTrend"]]["MadeMoney"] += 1
                                alldata[op["predictionValue"]][atime][op["volatility"]][op["overallTrend"]][op["endTrend"]]["Correct"] += 1
                        elif actualchange < -0.0:   
                            alldata[op["predictionValue"]][atime][op["volatility"]][op["overallTrend"]][op["endTrend"]]["Correct"] += 1
                
            newsitems += 1
        blockpos += blocksize
    opinion.closePool()

    databaseconnection.close()
    
#     for pt in pts:
#         for tt in tts:
#             for vt in vts:
#                 for ot in ots:
#                     for et in ets:
#                         
#                         if alldatacounts[tt][vt][ot][et] > 0:
#                             fulldata.append((pt, tt, vt, ot, et, alldatacounts[tt][vt][ot][et], alldata[tt][vt][ot][et] / alldatacounts[tt][vt][ot][et]))
#                         else:
#                             fulldata.append((tt, vt, ot, et, 0, 0))
#                     

    with open("../NewsData/TSO/TSO_EndvOverall_Averages.csv", "w") as MMfile:
        MMfile.write("Made Money Averages," + ",".join([str(et) for et in ets]) + "\n")
        for ot in ots:
            temp = str(ot) + ","
            for et in ets:
                asum = 0
                asumcount = 0
                
                for pt in pts:
                    for tt in tts:
                        for vt in vts:
                            asum += alldata[pt][tt][vt][ot][et]["MadeMoney"]
                            asumcount += alldata[pt][tt][vt][ot][et]["ShouldBuy"]
                temp += str(asum / asumcount) + ","
            temp += "\n"
            MMfile.write(temp)        
                   
#     with open("../NewsData/TSO/MM_MultiOpinionFullValues.csv", "w") as FVfile:
#         FVfile.write("Time Since Release, Volatility, Overall, End, Count, Made Money %\n")
#         for adata in fulldata:
#             FVfile.write(",".join([str(d) for d in adata]) + "\n")
#
       
    with open("../NewsData/TSO/MadeMoneyRatio_EvO.csv".format(int(tt)), "w") as MMRfile, open("../NewsData/TSO/ShouldBuyRatio_EvO.csv".format(int(tt)), "w") as SBRfile, open("../NewsData/TSO/CorrectRatio_EvO.csv".format(int(tt)), "w") as CRfile: 
        
        toprows = ",,,TIME," + (",,").join([(str(tt) + ",")*len(ets) for tt in tts]) + "\n"
        toprows += ",,end threshold,," + ",".join([str(et) for et in ets]) + (",,," + ",".join([str(et) for et in ets]))*(len(tts) - 1) + "\n"
        toprows += ",,,,\nPREDICTION THRESHOLD,overall threshold,,,\n"
        MMRfile.write(toprows)
        SBRfile.write(toprows)
        CRfile.write(toprows)
        
        for pt in pts:
            for vt in vts:
                for ot in ots:
                    mmrtemp = str(pt) + "," + str(ot) + ",,,"
                    sbrtemp = str(pt) + "," + str(ot) + ",,,"
                    crtemp = str(pt) + "," + str(ot) + ",,,"
                    
                    for tt in tts:
                        for et in ets:
                            if alldata[pt][tt][vt][ot][et]["ShouldBuy"]:
                                mmrtemp += str(alldata[pt][tt][vt][ot][et]["MadeMoney"] / alldata[pt][tt][vt][ot][et]["ShouldBuy"]) + ","
                            else:
                                mmrtemp += "None,"
                                
                            if alldata[pt][tt][vt][ot][et]["Total"]:
                                sbrtemp += str(alldata[pt][tt][vt][ot][et]["ShouldBuy"] / alldata[pt][tt][vt][ot][et]["Total"]) + ","
                                crtemp += str(alldata[pt][tt][vt][ot][et]["Correct"] / alldata[pt][tt][vt][ot][et]["Total"]) + ","
                            else:
                                sbrtemp += "None,"
                                crtemp += "None,"
                                
                        mmrtemp += ",,"  
                        sbrtemp += ",,"  
                        crtemp += ",,"  
                
                    MMRfile.write(mmrtemp + "\n")
                    SBRfile.write(sbrtemp + "\n")
                    CRfile.write(crtemp + "\n")
                    
            MMRfile.write("\n\n\n")
            SBRfile.write("\n\n\n")
            CRfile.write("\n\n\n")
                  
def MPmutliValue(aDatabaseName):
    module_logger.info("\n\n\n\n\n\n\n\n\n\n\n\n")
    
    ets = intervals(-0.00, 0.005, 5, includeNone=True)
    ots = intervals(-0.00, 0.005, 5, includeNone=True)
    vts = [None]#intervals(.1, 1, 10, includeNone=True)
    tts = intervals(0, 12*60*60, 3)
    pts = intervals(0, 0.2, 2, includeNone=True)
    
    alldata = {}
#    fulldata = []
    
    for pt in pts:
        alldata[pt] = {}        
        for tt in tts:
            alldata[pt][tt] = {}
            for vt in vts:
                alldata[pt][tt][vt] = {}
                for ot in ots:
                    alldata[pt][tt][vt][ot] = {}
                    for et in ets:
                        alldata[pt][tt][vt][ot][et] = {"Total":0, "ShouldBuy":0, "MadeMoney":0, "Correct":0}
                        
    
    databaseconnection = sqlite3.connect(aDatabaseName)
    cursor = databaseconnection.cursor()
        
    limit = 1000000
    blockpos = 0
    blocksize = 2500
    newsitems = 1
    
    somopool = pool.Pool()
    
    while newsitems > 0 and blockpos < limit:
        newsitems = 0
        cursor.execute("SELECT * FROM Articles WHERE LCount>1000 LIMIT ? OFFSET ? ", (blocksize, blockpos))
        module_logger.info("-- Starting entries {0} - {1} --".format(blockpos, blockpos + blocksize) )
        for articledetails in cursor:
            prediction = ThreadedBoWIDFI.ValueTimes.loads(articledetails[7])
            actual = ThreadedBoWIDFI.ValueTimes.loads(articledetails[8])
            if prediction != None and actual != None:
                timebar = 4*60*60
                actualChanges = {}
                
                for t in tts:
                    ptime = findClosest(actual, t)
                    actime = findClosest(actual, ptime + timebar)
                    actualChanges[ptime] = actual[actime] - actual[ptime]
                
                opinions = stockOpintionMultiOpinion(somopool, prediction, timebar, tts, eTs=ets, oTs=ots, vTs=vts, pTs=pts)
                pass
                for op in opinions:
                    alldata[op["predictionValue"]][op["time"]][op["volatility"]][op["overallTrend"]][op["endTrend"]]["Total"] += 1
                    if op["ShouldBuy"]:
                        alldata[op["predictionValue"]][op["time"]][op["volatility"]][op["overallTrend"]][op["endTrend"]]["ShouldBuy"] += 1
                        if actualChanges[op["time"]] > 0.00:
                            alldata[op["predictionValue"]][op["time"]][op["volatility"]][op["overallTrend"]][op["endTrend"]]["MadeMoney"] += 1
                            alldata[op["predictionValue"]][op["time"]][op["volatility"]][op["overallTrend"]][op["endTrend"]]["Correct"] += 1
                    elif actualChanges[op["time"]] < -0.0:   
                        alldata[op["predictionValue"]][op["time"]][op["volatility"]][op["overallTrend"]][op["endTrend"]]["Correct"] += 1
                
            newsitems += 1
        blockpos += blocksize
    somopool.close()
    somopool.join()
    databaseconnection.close()

    with open("../NewsData/TSO/TSO_EndvOverall_Averages.csv", "w") as MMfile:
        MMfile.write("Made Money Averages," + ",".join([str(et) for et in ets]) + "\n")
        for ot in ots:
            temp = str(ot) + ","
            for et in ets:
                asum = 0
                asumcount = 0
                
                for pt in pts:
                    for tt in tts:
                        for vt in vts:
                            asum += alldata[pt][tt][vt][ot][et]["MadeMoney"]
                            asumcount += alldata[pt][tt][vt][ot][et]["ShouldBuy"]
                if asumcount:
                    temp += str(asum / asumcount) + ","
                else:
                    temp += "inf,"
            temp += "\n"
            MMfile.write(temp)        
                   
    with open("../NewsData/TSO/MadeMoneyRatio_EvO.csv".format(int(tt)), "w") as MMRfile, open("../NewsData/TSO/ShouldBuyRatio_EvO.csv".format(int(tt)), "w") as SBRfile, open("../NewsData/TSO/CorrectRatio_EvO.csv".format(int(tt)), "w") as CRfile: 
        
        toprows = ",,,TIME," + (",,").join([(str(tt) + ",")*len(ets) for tt in tts]) + "\n"
        toprows += ",,end threshold,," + ",".join([str(et) for et in ets]) + (",,," + ",".join([str(et) for et in ets]))*(len(tts) - 1) + "\n"
        toprows += ",,,,\nPREDICTION THRESHOLD,overall threshold,,,\n"
        MMRfile.write(toprows)
        SBRfile.write(toprows)
        CRfile.write(toprows)
        
        for pt in pts:
            for vt in vts:
                for ot in ots:
                    mmrtemp = str(pt) + "," + str(ot) + ",,,"
                    sbrtemp = str(pt) + "," + str(ot) + ",,,"
                    crtemp = str(pt) + "," + str(ot) + ",,,"
                    
                    for tt in tts:
                        for et in ets:
                            if alldata[pt][tt][vt][ot][et]["ShouldBuy"]:
                                mmrtemp += str(alldata[pt][tt][vt][ot][et]["MadeMoney"] / alldata[pt][tt][vt][ot][et]["ShouldBuy"]) + ","
                            else:
                                mmrtemp += "None,"
                                
                            if alldata[pt][tt][vt][ot][et]["Total"]:
                                sbrtemp += str(alldata[pt][tt][vt][ot][et]["ShouldBuy"] / alldata[pt][tt][vt][ot][et]["Total"]) + ","
                                crtemp += str(alldata[pt][tt][vt][ot][et]["Correct"] / alldata[pt][tt][vt][ot][et]["Total"]) + ","
                            else:
                                sbrtemp += "None,"
                                crtemp += "None,"
                                
                        mmrtemp += ",,"  
                        sbrtemp += ",,"  
                        crtemp += ",,"  
                
                    MMRfile.write(mmrtemp + "\n")
                    SBRfile.write(sbrtemp + "\n")
                    CRfile.write(crtemp + "\n")
                    
            MMRfile.write("\n\n\n")
            SBRfile.write("\n\n\n")
            CRfile.write("\n\n\n")      

if __name__ == '__main__':
    singleValue("../NewsData/BoW_IDF_Learner5.db")
    #mutliValue("../NewsData/BoW_IDF_Learner4.db")
    #MPmutliValue("../NewsData/BoW_IDF_Learner4.db")
        