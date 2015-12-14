'''
Created on Nov 5, 2015

@author: Sean
'''

import threading
import Helpers
import IDFinanceSQLWriter
import IDFinanceData
import IDFinanaceClient

class IDFinanceClientIntelligent(IDFinanceSQLWriter.IDFinanceSQLWriter):
    '''
    classdocs
    '''
    
    def __init__(self, aCompanyInformationList, aSQLConnection, UpdateGap=0):
        self.status = "OPEN"
        self.executelock = threading.RLock()
        
        self.timer = None
        #the update gap should really never be less than 10 min due to the time requirements and there's no real point to update that frequently
        if UpdateGap > 60*10:
            self.timer = Helpers.IntervalTimer(UpdateGap, self.update)
            self.timer.start()
        
        self.companyinfolist = aCompanyInformationList
        
        self.idfcidbconnection = aSQLConnection
            
    def update(self, service="MAIN"):
        if(service.upper() == "Main"):
            gthread = threading.Thread(target=self.update, args=("GOOGLE"))
            ythread = threading.Thread(target=self.update, args=("YAHOO"))
            gthread.join()
            ythread.join()
            
        elif(service.upper() =="GOOGLE"):
            IDFinanaceClient.GoogleIDFinanceClient(self).getData(self.companyinfolist)
            
        elif(service.upper() =="YAHOO"):
            IDFinanaceClient.YahooIDFinanceClient(self).getData(self.companyinfolist)
        

    def hasProperStart(self, ticker, pubdateTSE, thresholddays=5, service="GOOGLE"):
        tablename = (service + ticker).upper()
        rows = self.idfcidbconnection.execute("SELECT * FROM [{0}] WHERE Timestamp>=? LIMIT 1;".format(tablename), (pubdateTSE,))
        if not rows:
            return True
        
        #The starting data must be within 5 days of the actual pub date
        if rows[0][0] - pubdateTSE > 60*60*24*thresholddays:
            return False
        return True
        
    def getData(self, ticker, timeSinceEpoch, increment=30, count=39, service="GOOGLE"):
        valtimes = {}
        
        tablename = (service + ticker).upper()
        readblocksize = 100
        blocks = [[]]
 
        rows = self.idfcidbconnection.execute("SELECT * FROM [{0}] WHERE Timestamp>=? LIMIT 1;".format(tablename), (timeSinceEpoch,))
        if not rows:
            return None
        
        startvalue = (rows[0][1] + rows[0][4]) / 2.0
        blockpos = 1
        rowsperexecute = 1
        
        while rowsperexecute > 0 and len(blocks) < count:
            rowsperexecute = 0
            if len(blocks[-1]) >= increment:
                blocks.append([])
                
            rows = self.idfcidbconnection.execute("SELECT * FROM [{0}] WHERE Timestamp>? LIMIT ? OFFSET ?".format(tablename), (timeSinceEpoch, readblocksize, blockpos, ))

            for row in rows:
                if not blocks[-1]:
                    blocks[-1].append(row)
                else:
                    timediff = abs(blocks[-1][-1][0] - row[0])
                    
                    if (timediff > 2*60):
                        blocks.append([])
                    blocks[-1].append(row)
                        
                if len(blocks[-1]) >= increment:
                    blocks.append([])
                rowsperexecute += 1

            blockpos += readblocksize
        
        blocks = blocks[:-1][:count]
        
        if len(blocks) != count:
            return None
        
        i = 0
        
        for block in blocks:
            tb = block[:increment]
            
            runningaverage = 0
            weighttotal = 0
            
            for row in tb:
                runningaverage += float(row[1]+row[4])/2.0 * float(row[5])
                weighttotal += float(row[5])
            
            avgstockvalue = runningaverage / weighttotal
            valtimes[i*increment*60] = (avgstockvalue - startvalue) / startvalue * 100.0
            i += 1
            
        return Helpers.ValueTimes(valtimes)
        
    def write(self, aIDFinanceData):
        """ write out the given finance data to the necessary database """
        if(isinstance(aIDFinanceData, IDFinanceData.IDFinanceData)):
            aIDFinanceData = [aIDFinanceData]
        
        for idfd in aIDFinanceData:
            if(idfd.data != None):
                tablename = idfd.service + idfd.ticker
                
                self.idfcidbconnection.execute("CREATE TABLE IF NOT EXISTS [{0}] (Timestamp int UNIQUE, Close float, High float, Low float, Open float, Volume int)".format(tablename))
                for row in idfd.data:
                    self.idfcidbconnection.execute("INSERT or IGNORE INTO [{0}] VALUES (?,?,?,?,?,?)".format(tablename), row)
                    
    def close(self):
        if(self.status != "CLOSED"):
            self.status = "CLOSED"
            
            if self.timer != None:
                self.timer.cancel()
            
            self.idfcidbconnection.close()
        
    def __del__(self):
        self.close()
        
class IDFinanceClientIntelligentChooseAverage(IDFinanceClientIntelligent):
    def __init__(self, aCompanyInformationList, aSQLConnection, UpdateGap=0, leftMost=1, rightMost=1):
        super().__init__(aCompanyInformationList, aSQLConnection, UpdateGap=UpdateGap)
        self.LeftMost=leftMost
        self.RightMost=rightMost
        
    def getData(self, ticker, timeSinceEpoch, increment=30, count=39, service="GOOGLE"):
        valtimes = {}
        blocks = [[]]
        count += 1
        tablename = (service + ticker).upper()
        
        readblocksize = 250
        blockpos = 1
        rowsperexecute = 1
 
        rows = self.idfcidbconnection.execute("SELECT * FROM [{0}] WHERE Timestamp>=? ORDER BY Timestamp ASC LIMIT 1".format(tablename), (timeSinceEpoch,))
        if not rows:
            return None
        
        startvalue = rows[0][4]
        
        while rowsperexecute > 0 and len(blocks) < count:
            rowsperexecute = 0
            if len(blocks[-1]) >= increment:
                blocks.append([])
                
            rows = self.idfcidbconnection.execute("SELECT * FROM [{0}] WHERE Timestamp>?  ORDER BY Timestamp ASC LIMIT ? OFFSET ?".format(tablename), (timeSinceEpoch, readblocksize, blockpos, ))

            for row in rows:
                if not blocks[-1]:
                    blocks[-1].append(row)
                else:
                    timediff = abs(blocks[-1][-1][0] - row[0])
                    
                    if (timediff > 2*60):
                        blocks.append([])

                    if (timediff > 59):  
                        blocks[-1].append(row)
                        
                if len(blocks[-1]) >= increment:
                    blocks.append([])
                rowsperexecute += 1

            blockpos += readblocksize
        
        blocks = blocks[:count]
        
        if len(blocks) != count:
            return None
        
        i = 0
        for i in range(len(blocks)-1):
            asum = sum([val[2]*val[5] for val in blocks[i][:-(self.LeftMost)]]) + sum([val[2]*val[5] for val in blocks[i+1][:(self.RightMost)]])
            weights = sum([val[5] for val in blocks[i][:-(self.LeftMost)]]) + sum([val[5] for val in blocks[i+1][:(self.RightMost)]])
            
            valtimes[i*increment*60] = (asum / weights - startvalue) / startvalue * 100.0
            
        return Helpers.ValueTimes(valtimes)
    
    
class IDFinanceClientIntelligentNonAverage(IDFinanceClientIntelligent):
    def __init__(self, aCompanyInformationList, aSQLConnection, UpdateGap=0):
        super(IDFinanceClientIntelligentNonAverage, self).__init__(aCompanyInformationList, aSQLConnection, UpdateGap=0)
    def getData(self, ticker, timeSinceEpoch, increment=30, count=39, service="GOOGLE"):
        valtimes = {}
        
        tablename = (service + ticker).upper()
        readblocksize = 100
        blocks = [[]]
 
        rows = self.idfcidbconnection.execute("SELECT * FROM [{0}] WHERE Timestamp>=? LIMIT 1;".format(tablename), (timeSinceEpoch,))
        if not rows:
            return None
        
        startvalue = rows[0][4]
        blockpos = 1
        rowsperexecute = 1
        
        while rowsperexecute > 0 and len(blocks) < count:
            rowsperexecute = 0
            if len(blocks[-1]) >= increment:
                blocks.append([])
                
            rows = self.idfcidbconnection.execute("SELECT * FROM [{0}] WHERE Timestamp>? LIMIT ? OFFSET ?".format(tablename), (timeSinceEpoch, readblocksize, blockpos, ))

            for row in rows:
                if not blocks[-1]:
                    blocks[-1].append(row)
                else:
                    timediff = abs(blocks[-1][-1][0] - row[0])
                    
                    if (timediff > 2*60):
                        blocks.append([])
                    blocks[-1].append(row)
                        
                if len(blocks[-1]) >= increment:
                    blocks.append([])
                rowsperexecute += 1

            blockpos += readblocksize
        
        blocks = blocks[:-1][:count]
        
        if len(blocks) != count:
            return None
        
        i = 0
        
        for block in blocks:
            valtimes[i*increment*60] = (block[:increment][-1][4] - startvalue) / startvalue * 100.0
            i += 1
            
        return Helpers.ValueTimes(valtimes)