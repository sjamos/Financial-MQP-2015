'''
Created on Nov 11, 2015

@author: Sean
'''
import sqlite3, csv, datetime, pytz

def median(aList):
    aList = sorted(aList)
    if len(aList) == 0:
        return None
    elif len(aList) == 1:
        m = aList[0]
        l1 = []
        l2 = []
    elif len(aList) % 2 == 1:
        middle = int((len(aList)-1)/2)
        m = aList[middle]
        l1 = aList[0:middle]
        l2 = aList[middle+1:]
    else:
        middle = int(len(aList)/2)
        m = (aList[middle] + aList[middle-1]) / 2.0
        l1 = l1 = aList[0:middle]
        l2 = aList[middle:]
        
    return (m, l1, l2)

def updatedates():
    databaseconnection = sqlite3.connect("../NewsData/News.db")
    databaseconnection.row_factory = sqlite3.Row
    cursor = databaseconnection.cursor()

    update = {}
    updatel = []
    cursor.execute("SELECT pubdate FROM News")
    for row in cursor:
        try:
            dt = pytz.UTC.localize(datetime.datetime.strptime(row["pubdate"], "%a, %d %b %Y %H:%M:%S %Z"))
            update[row["pubdate"]] = dt.isoformat()
        except: 
            pass
     
    for old in update:
        updatel.append((update[old], old,))
        
    count = 0
    print(datetime.datetime.now().isoformat() + " --- " + "Starting to write")
    while count < len(updatel):
        lcount = count
        count += 200
        if count > len(updatel):
            count = len(updatel)
        cursor.executemany("UPDATE News SET pubdate=? WHERE pubdate=?;", updatel[lcount:count])
        databaseconnection.commit()
        print(datetime.datetime.now().isoformat() + " --- {0}/{1}".format(count, len(updatel)))
        
    databaseconnection.close()
        
def cleandates():
    databaseconnection = sqlite3.connect("../NewsData/News.db")
    databaseconnection.row_factory = sqlite3.Row
    cursor = databaseconnection.cursor()
    startofcollection = datetime.datetime(2015, 10, 1).isoformat()
    cursor.execute("DELETE FROM News WHERE pubdate<=?", (startofcollection,))
    databaseconnection.commit()

            
def writeDatesHistogram():
    sp500file = open("../SP5002.csv", 'r')
    tickerdates = {}
    reader = csv.reader(sp500file, dialect='excel')
    
    donotread = False
    for row in reader:
        if "Ticker" not in row and not donotread:
            tickerdates[row[0]] = {"daycounts":{}, "weekcounts":{}, "dates":[]}
            
    sp500file.close()
    
    databaseconnection = sqlite3.connect("../NewsData/News.db")
    databaseconnection.row_factory = sqlite3.Row
    cursor = databaseconnection.cursor()
    
    dayofweekcount = [0,0,0,0,0,0,0]
    startofcollection = datetime.datetime(2015, 10, 6).isoformat()
    for symbol in tickerdates:
        print(datetime.datetime.now().isoformat() + " ---" + symbol)
        cursor.execute("SELECT pubdate FROM News WHERE ticker=?", (symbol,))
        for row in cursor:
            if row["pubdate"] >= startofcollection:
                dt = datetime.datetime.strptime(row["pubdate"].replace(":", ""), "%Y-%m-%dT%H%M%S%z").astimezone()
                d = dt.date()
                if d not in tickerdates[symbol]["daycounts"]:
                    tickerdates[symbol]["daycounts"][d] = 0
                tickerdates[symbol]["daycounts"][d] += 1
                w = d - datetime.timedelta(d.weekday())
                if w not in tickerdates[symbol]["weekcounts"]:
                    tickerdates[symbol]["weekcounts"][w] = 0
                tickerdates[symbol]["weekcounts"][w] += 1
                
                dayofweekcount[dt.weekday()] += 1
            
    
    databaseconnection.close()
    
    analysisfile = open("../NewsData/newsdatesinfo.csv", "w")
    
    analysisfile.write("Percentage of Articles published by day\nMonday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday\n")
    asum = sum(dayofweekcount)
    dayofweekpercent = map(lambda x: x/asum, dayofweekcount)
    analysisfile.write(",".join(map(str, dayofweekpercent)) + "\n\n\n")

    analysisfile.write("Box and Whisker data by ticker over a day\n Ticker, min, q1, median, q3, max, mean, Total Articles, Total Days, Start, End\n")
    
    for symbol in tickerdates:
        counts = list(tickerdates[symbol]["daycounts"].values())
        d1 = min(tickerdates[symbol]["daycounts"].keys())
        d2 = datetime.date.today()
        diff = d2 - d1
        for i in range(diff.days + 1):
            if (d1 + datetime.timedelta(i)) not in tickerdates[symbol]["daycounts"]:
                counts.append(0)
        mls = median(counts)
        q1ls = median(mls[1])
        q3ls = median(mls[2])
        
        data = (symbol, min(counts), q1ls[0], mls[0], q3ls[0], max(counts), sum(counts)/len(counts), sum(counts), len(tickerdates[symbol]["daycounts"]), d1, d2)
        analysisfile.write(",".join(map(str, data)) + "\n")
        
    analysisfile.write("\n\n\nBox and Whisker data by ticker over a week\n  Ticker, min, q1, median, q3, max, mean, Total Articles, Total Days, Start, End\n")
    
    for symbol in tickerdates:
        counts = list(tickerdates[symbol]["weekcounts"].values())
        d1 = min(tickerdates[symbol]["weekcounts"].keys())
        d2 = datetime.date.today()
        diff = d2 - d1
        for i in range(int((diff.days + 1)/7)):
            if (d1 + datetime.timedelta(i*7)) not in tickerdates[symbol]["weekcounts"]:
                counts.append(0)
        mls = median(counts)
        q1ls = median(mls[1])
        q3ls = median(mls[2])
        
        data = (symbol, min(counts), q1ls[0], mls[0], q3ls[0], max(counts), sum(counts)/len(counts), sum(counts), len(tickerdates[symbol]["weekcounts"]), d1, d2)
        analysisfile.write(",".join(map(str, data)) + "\n")
    
    analysisfile.close()
    
    
if __name__ == '__main__':
    #updatedates()
    #cleandates()
    writeDatesHistogram()