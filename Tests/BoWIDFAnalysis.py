'''
Created on Nov 8, 2015

@author: Sean
'''
import sqlite3
import BoWIDFI
import Helpers

def writeToCSVs():
    csvfile = open("../NewsData/articlepredictions.csv", "w")
    csvfile.write("PCount, " + "Prediction," * 100 + ",,,," + "Actual," * 100 + ",,,," + "Diff," * 100 + ",,,,\n" )
    databaseconnection = sqlite3.connect("../NewsData/BoW_IDF_Learner5.db")
    cursor = databaseconnection.cursor()
    limit = 1000000
    blockpos = 0
    blocksize = 2000
    newsitems = 1
    while newsitems > 0 and blockpos < limit:
        newsitems = 0
        cursor.execute("SELECT * FROM Articles LIMIT ? OFFSET ?", (blocksize, blockpos))
        print("""-------------------------------------------------------\n
                 \r     -------  Starting entries {0} - {1}  -------\n
                 \r-------------------------------------------------------\n""".format(blockpos, blockpos + blocksize) )
        for articledetails in cursor:
            prediction = Helpers.ValueTimes.loads(articledetails[7])
            actual = Helpers.ValueTimes.loads(articledetails[8])
                
            if prediction != None and actual != None:
                diff = actual - prediction 
                csvfile.write("{0},".format(articledetails[9]))
                for key in sorted(list(prediction.valuetimes)):
                    csvfile.write("{0},".format(prediction.valuetimes[key]))
                csvfile.write(",,,,")
                for key in sorted(list(actual.valuetimes)):
                    csvfile.write("{0},".format(actual.valuetimes[key]))
                csvfile.write(",,,,")
                for key in sorted(list(diff.valuetimes)):
                    csvfile.write("{0},".format(diff.valuetimes[key]))
                csvfile.write("\n")

            newsitems += 1
        blockpos += blocksize
    
    databaseconnection.close()
    csvfile.close()
    
    csvfile = open("../NewsData/wordpredictions.csv", "w")
    databaseconnection = sqlite3.connect("../NewsData/BoW_IDF_Learner5.db")
    csvfile.write("Word, Document Count, Total Count, Predictions,\n")
    cursor = databaseconnection.cursor()
    limit = 0
    blockpos = 0
    blocksize = 2000
    newsitems = 1
    while newsitems > 0 and blockpos < limit:
        newsitems = 0
        cursor.execute("SELECT * FROM Words LIMIT ? OFFSET ?", (blocksize, blockpos))
        print("""-------------------------------------------------------\n
                 \r     -------  Starting entries {0} - {1}  -------\n
                 \r-------------------------------------------------------\n""".format(blockpos, blockpos + blocksize) )
        for worddetails in cursor:
            prediction = Helpers.ValueTimes.loads(worddetails[3])
            if prediction != None:
                csvfile.write("{0},{1},{2},".format(worddetails[0].encode('ascii','ignore'),worddetails[1],worddetails[2]))
                for key in sorted(prediction.valuetimes):
                    csvfile.write("{0},".format(prediction.valuetimes[key]))
                csvfile.write("\n")
            elif worddetails[0] == BoWIDFI.BoWIDFIntelligence.masterWord:
                csvfile.write("{0},{1},{2},\n".format(worddetails[0].encode('ascii','ignore'),worddetails[1],worddetails[2]))
            newsitems += 1
        blockpos += blocksize
    
    databaseconnection.close()
    csvfile.close()
    
def writeData():
    analysisfile = open("../NewsData/analysis.csv", "w")
    averageanalysisfile = open("../NewsData/averageanalysis.csv", "w")
    analysisfile.write("PCount, Euclidean Distance, Euclidean Area, UP Down - 0, UP Down - .1, UP Down - .25, UP Down - 0.5, Shortest Euclidean Distance - A, Shortest Euclidean Area - A, Shortest Euclidean Distance - P, Shortest Euclidean Area - P, RMSE, MAPE\n" )
    averageanalysisfile.write("PCount, Euclidean Distance, Euclidean Area, UP Down - 0, UP Down - .1, UP Down - .25, UP Down - 0.5, Shortest Euclidean Distance - A, Shortest Euclidean Area - A, Shortest Euclidean Distance - P, Shortest Euclidean Area - P, RMSE, MAPE\n" )
    
    towrite = []
    
    
    
    databaseconnection = sqlite3.connect("../NewsData/BoW_IDF_Learner4.db")
    cursor = databaseconnection.cursor()
    limit = 0
    blockpos = 0
    blocksize = 500
    newsitems = 1
    while newsitems > 0 and blockpos < limit:
        newsitems = 0
        cursor.execute("SELECT * FROM Articles LIMIT ? OFFSET ?", (blocksize, blockpos))
        print("""-------------------------------------------------------\n
                 \r     -------  Starting entries {0} - {1}  -------\n
                 \r-------------------------------------------------------\n""".format(blockpos, blockpos + blocksize) )
        for articledetails in cursor:
            prediction = Helpers.ValueTimes.loads(articledetails[7])
            actual = Helpers.ValueTimes.loads(articledetails[8])
            
            if prediction != None and actual != None:
                towrite.append((articledetails[9], 
                            euclideanDistanceValue(actual, prediction), areaDiffValue(actual, prediction),
                            upDownValue(actual, prediction), upDownValue(actual, prediction, 0.1), 
                            upDownValue(actual, prediction, 0.25), upDownValue(actual, prediction, 0.5),
                            shortestEuclideanDistance(actual, prediction), shortestEuclideanArea(actual, prediction), 
                            shortestEuclideanDistance(prediction, actual), shortestEuclideanArea(prediction, actual),
                            rmse(prediction, actual), mape(prediction, actual)))
            newsitems += 1
        blockpos += blocksize
    
    towrite.sort(key=lambda x: x[0])
    
    count = 0
    averagesize = 100
    empty = [0,0,0,0,0,0,0,0,0,0,0,0,0]
    average = empty[:]
    
    for row in towrite:
        if count % averagesize == 0:
            t = ""
            for i in range(len(average)):
                t += "{0},".format(average[i])
            averageanalysisfile.write(t + "\n")
            
            average = empty[:]
            
        count += 1
        
        t = ""
        for i in range(len(row)):
            t += "{0},".format(row[i])
        analysisfile.write(t + "\n")
        
        for i in range(len(average)):
            average[i] += row[i]/averagesize
        
    databaseconnection.close()
    analysisfile.close()
    averageanalysisfile.close()
        
    
def euclideanDistanceValue(aActualTV, aPredictionTV):
    diff = aActualTV - aPredictionTV
    totaldist = 0
    for time in diff:
        totaldist += abs(diff[time])
    return totaldist
        
def lineArea(x1, y1, x2, y2):
    if x1 == x2:
        return 0
    m = ( y1 - y2 )/( x1 - x2 )
    b = y1 - m * x1
    if m == 0:
        area = abs( (x1 - x2) * y1)
    elif (y1 > 0 and y2 > 0) or (y1 < 0 and y2 < 0):
        area = abs( ( m/2.0*x2*x2 + b*x2 ) - ( m/2.0*x1*x1 + b*x1 ) )
    else:
        x0 = -b/m
        area = abs( ( m/2.0*x0*x0 + b*x0 ) - ( m/2.0*x1*x1 + b*x1 ) )
        area += abs( ( m/2.0*x2*x2 + b*x2 ) - ( m/2.0*x0*x0 + b*x0 ) )
    return area
    
def pointDist(x1, y1, x2, y2):
    return pow( pow(x2-x1, 2) + pow(y2-y2, 2), 0.5)
    
def areaDiffValue(aActualTV, aPredictionTV):
    diff = aActualTV - aPredictionTV
    keys = diff.keys()
    totalarea = 0
    
    for i in range(len(keys)-1):
        totalarea += lineArea(keys[i], diff[keys[i]], keys[i+1], diff[keys[i+1]])
        
    return totalarea

def upDownValue(aActualTV, aPredictionTV, threshold=0):
    total = 0
    for time in aActualTV:
        if (aActualTV[time] > threshold and aPredictionTV[time] > threshold):
            total += 1
        elif (aActualTV[time] < -threshold and aPredictionTV[time] < -threshold):
            total += 1
        elif (aActualTV[time] <= threshold and aPredictionTV[time] <= threshold) and (aActualTV[time] >= -threshold and aPredictionTV[time] >= -threshold):
            total += 1
            
    return total
    
def closestLineSegmentPoint(xp, yp, x1, y1, x2, y2):
    px = x2-x1
    py = y2-y1
    if px == 0 and py == 0:
        pass
    something = px*px + py*py

    u =  ((xp - x1) * px + (yp - y1) * py) / float(something)

    if u > 1:
        u = 1
    elif u < 0:
        u = 0

    x = x1 + u * px
    y = y1 + u * py

    return (x,y)

def shortestEuclideanDistance(aActualTV, aPredictionTV):
    diff = aActualTV - aPredictionTV
    keys = diff.keys()
    totaldist = 0
    shortestpoints = []

    shortestpoints.append( closestLineSegmentPoint( keys[0], aActualTV[keys[0]], keys[0], aActualTV[keys[0]], keys[1], aActualTV[keys[1]] ) )
    for i in range(1, len(keys)-1):
        p1 = closestLineSegmentPoint( keys[i], aActualTV[keys[i]], keys[i-1], aActualTV[keys[i-1]], keys[i], aActualTV[keys[i]] )
        p2 = closestLineSegmentPoint( keys[i], aActualTV[keys[i]], keys[i], aActualTV[keys[i]], keys[i+1], aActualTV[keys[i+1]] )

        if pointDist(keys[i], aActualTV[keys[i]], p1[0], p1[1]) < pointDist(keys[i], aActualTV[keys[i]], p2[0], p2[1]):
            shortestpoints.append( p1 )
        else:
            shortestpoints.append( p2 )
           
    shortestpoints.append( closestLineSegmentPoint( keys[-1], aActualTV[keys[-1]], keys[-2], aActualTV[keys[-2]], keys[-1], aActualTV[keys[-1]] ) ) 
    
    for i in range(len(shortestpoints)-1):
        totaldist += abs( shortestpoints[i][1] - shortestpoints[i+1][1])
        
    return totaldist

def shortestEuclideanArea(aActualTV, aPredictionTV):
    diff = aActualTV - aPredictionTV
    keys = diff.keys()
    totalarea = 0
    shortestpoints = []

    shortestpoints.append( closestLineSegmentPoint( keys[0], aActualTV[keys[0]], keys[0], aActualTV[keys[0]], keys[1], aActualTV[keys[1]] ) )
    for i in range(1, len(keys)-1):
        p1 = closestLineSegmentPoint( keys[i], aActualTV[keys[i]], keys[i-1], aActualTV[keys[i-1]], keys[i], aActualTV[keys[i]] )
        p2 = closestLineSegmentPoint( keys[i], aActualTV[keys[i]], keys[i], aActualTV[keys[i]], keys[i+1], aActualTV[keys[i+1]] )

        if pointDist(keys[i], aActualTV[keys[i]], p1[0], p1[1]) < pointDist(keys[i], aActualTV[keys[i]], p2[0], p2[1]):
            shortestpoints.append( p1 )
        else:
            shortestpoints.append( p2 )
           
    shortestpoints.append( closestLineSegmentPoint( keys[-1], aActualTV[keys[-1]], keys[-2], aActualTV[keys[-2]], keys[-1], aActualTV[keys[-1]] ) ) 
    
    for i in range(len(shortestpoints)-1):
        totalarea += lineArea(shortestpoints[i][0], shortestpoints[i][1], shortestpoints[i+1][0], shortestpoints[i+1][1])
        
    return totalarea

def rmse(aActualTV, aPredictionTV):
    diff = aActualTV - aPredictionTV
    
    asum = 0
    for time in diff:
        asum += pow(aActualTV[time] - aPredictionTV[time], 2)
    
    return pow(asum/len(diff), 0.5)
    
def mape(aActualTV, aPredictionTV):
    diff = aActualTV - aPredictionTV

    asum = 0
    for time in diff:
        if aPredictionTV[time] != 0:
            asum += abs(aActualTV[time] - aPredictionTV[time]) / aPredictionTV[time]
    
    return asum/len(diff)

if __name__ == '__main__':
    writeToCSVs()
    #writeData()
    