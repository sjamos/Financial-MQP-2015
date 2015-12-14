'''
Created on Nov 2, 2015

@author: Sean
'''

import queue
import re
from builtins import str
from math import log
import time
import newspaper
import nltk
import math
import Logger
from Helpers import CustomSQLFunction, ValueTimes, TSSQLConnection, IntervalTimer, ThreadManager, MyConfig

module_logging = Logger.getLogger(__name__)

class BoWIDFIntelligence:
    
    masterWord = "__MASTER_WORD__"
        
    wordTable = "Words"
    articleTable = "Articles"
    predictQueueTable = "ToPredictQueue"
    learnQueueTable = "ToLearnQueue"
    parseQueueTable = "ToParseQueue"
    
    predictLabel = "PREDICT"
    learnLabel = "LEARN"
    parseLabel = "PARSE"
    
    
    def __init__(self, aCompanyInformationList, aBoWIDFDatabaseName, aIDFinanceClientIntelligent,
                         LearningThreads=3, PredictingThreads=2, ParsingThreads=4):
        self.status = "OPEN"
        
        self.companyinformationlist = aCompanyInformationList
        
        self.bowidfdbconnection = TSSQLConnection(aBoWIDFDatabaseName)
        UpdateWord(self.bowidfdbconnection)
        UpdateArticle(self.bowidfdbconnection)
        UpdateWords(self.bowidfdbconnection)
        
        self.intradayconnection = aIDFinanceClientIntelligent
        
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                Word text UNIQUE, 
                                DocumentCount int, 
                                TotalCount int, 
                                Prediction text)""".format(self.wordTable), block=True)
        
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                        Service text,
                                        Ticker text,
                                        Title text,
                                        PubDate int,
                                        Link text,
                                        NewsID text UNIQUE,
                                        Content text,
                                        Prediction text,
                                        Actual text,
                                        PCount int)""".format(self.articleTable), block=True)
                
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                        Service text,
                                        Ticker text,
                                        Title text,
                                        PubDate text,
                                        Link text,
                                        NewsID text UNIQUE,
                                        FullContent text,
                                        Caller text)""".format(self.predictQueueTable), block=True)
        
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                        Service text,
                                        Ticker text,
                                        Title text,
                                        PubDate text,
                                        Link text,
                                        NewsID text UNIQUE,
                                        FullContent text,
                                        Caller text)""".format(self.learnQueueTable), block=True)
        
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                        Service text,
                                        Ticker text,
                                        Title text,
                                        PubDate text,
                                        Link text,
                                        NewsID text UNIQUE,
                                        FullContent text,
                                        Caller text)""".format(self.parseQueueTable), block=True)
                
                
        self.longlearnqueue = queue.Queue()
        self.longlearntimer = IntervalTimer(60*60,  self.__moveLongLearn2Regular)
        
        self.predicterManager = ThreadManager(self.predictLabel, PredictingThreads, self.__predicterWorker)
        self.learnerManager = ThreadManager(self.learnLabel, LearningThreads, self.__learnerWorker)
        self.parserManager = ThreadManager(self.parseLabel, ParsingThreads, self.__parserWorker)
        self.__tableQueues2Managers()
        self.longlearntimer.start()

    def predict(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, doBlock=False, doLearn=True):
        if self.__canPredictInput(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource):
            self.predicterManager.put((self.predictLabel, (aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource)))
        
    def learn(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource):
        if self.__canLearnInput(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource):
            self.learnerManager.put((self.learnLabel, (aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource)))
    
    def __canPredictInput(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource):
        return True
        
    def __canLearnInput(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource):
        return True
    
    def join(self):
        for _ in range(4):
            self.learnerManager.join()
            self.predicterManager.join()
            self.parserManager.join()
            
    def close(self, block=False):
        if(self.status != "CLOSED"):
            self.status = "CLOSED"
            
            if block:
                self.join()
                
            self.learnerManager.close()
            self.predicterManager.close()
            self.parserManager.close()
        
            self.longlearntimer.cancel()
            self.__moveLongLearn2Regular()
            
            self.__managers2TableQueues()
            
            self.bowidfdbconnection.close()
             
    def __del__(self):
        self.close()
    
    def __tableQueues2Managers(self):
        self.__tableQueue2Manager(self.predictQueueTable, self.predicterManager)
        self.__tableQueue2Manager(self.learnQueueTable, self.learnerManager)
        self.__tableQueue2Manager(self.parseQueueTable, self.parserManager)
        
    def __tableQueue2Manager(self, aTable, aManager):

        rows = self.bowidfdbconnection.execute("SELECT * FROM {0};".format(aTable), block=True)
        for row in rows:
            toput = (row[7], (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            aManager.put(toput)
        self.bowidfdbconnection.execute("DELETE FROM {0};".format(aTable), block=True)
        self.bowidfdbconnection.execute("VACUUM;", block=True)
    
    def __managers2TableQueues(self):
        self.__manager2TableQueue(self.predictQueueTable, self.predicterManager)
        self.__manager2TableQueue(self.learnQueueTable, self.learnerManager)
        self.__manager2TableQueue(self.parseQueueTable, self.parserManager)
    
    def __manager2TableQueue(self, aTable, aManager):
        try:
            while True:
                item = aManager.queue.get(block=False)[1]
                toput = (item[1][0], item[1][1], item[1][2], item[1][3], item[1][4], item[1][5], item[1][6], item[0],)
                self.bowidfdbconnection.execute("INSERT OR IGNORE INTO {0} VALUES (?,?,?,?,?,?,?,?)".format(aTable), toput)
                aManager.queue.task_done()
        except queue.Empty:
            pass
    
    def __predicterWorker(self, aQueue, aStopToken):
        while True:
            item = aQueue.get()
                                    
            if item[1] == aStopToken:
                break
            
            newsitem = self.__getNewsItemByID(item[1][1][5])
            if newsitem == None:
                self.parserManager.put(item[1])
            elif newsitem[7] == None:
                textinfo = self.__getTextInformation(newsitem[6])
                masterinfo = None
                wordcount = 0
                
                for word in textinfo:
                    wordcount += textinfo[word][0]
                    if word == self.masterWord:
                        masterinfo = textinfo[word]
                        
                weights = {}
                weighttotal = 0.0
                
                for word in textinfo:
                    if word != self.masterWord:
                        tf = float(textinfo[word][0]) / float(wordcount)
                        idf = log( 1.0 + (float(masterinfo[1][1]) / (1 + float(textinfo[word][1][1]))) )
                        tfidf = tf*idf
                        weights[word] = tfidf
                        weighttotal += tfidf
                        
                mastervaltime = self.__getEmptyPrediction()
                if weighttotal != 0:
                    for word in weights:
                        valtime = ValueTimes.loads(textinfo[word][1][3])
                        if weights[word] > 0 and not valtime.isZero():
                            mastervaltime = mastervaltime + valtime*((128.0 * weights[word]) / weighttotal)
                    mastervaltime = mastervaltime * (1 / 128.0)
                self.bowidfdbconnection.execute("UPDATEARTICLE {0} SET Prediction=? WHERE NewsID=?".format(self.articleTable), (mastervaltime, item[1][1][5],), block=True)
                
            if item[1][0] == self.learnLabel:
                self.learnerManager.put(item[1])

            aQueue.task_done()

        aQueue.task_done()
        
    def __learnerWorker(self, aQueue, aStopToken):
        while True:
            item = aQueue.get()
                                               
            if item[1] == aStopToken:
                break
        
            newsitem = self.__getNewsItemByID(item[1][1][5])
            if newsitem == None:
                self.parserManager.put(item[1])
            elif newsitem[7] == None or len(newsitem[7]) < 5:
                toput = (item[1][0], (item[1][1][0], item[1][1][1], item[1][1][2], item[1][1][3], item[1][1][4], item[1][1][5], None))
                self.predicterManager.put(toput)
            elif newsitem[8] == None:
                data = self.intradayconnection.getData(item[1][1][1], newsitem[3])
                if data == None:
                    toput = (item[1][0], (item[1][1][0], item[1][1][1], item[1][1][2], item[1][1][3], item[1][1][4], item[1][1][5], None))
                    self.longlearnqueue.put(toput)
                else:
                    tokens = self.__tokenize(newsitem[6])
                    self.bowidfdbconnection.execute("UPDATEWORDS", (tokens, data), block=True)
                    self.bowidfdbconnection.execute("UPDATE {0} SET Actual=? WHERE NewsID=?".format(self.articleTable), (data.dumps(), item[1][1][5],))
            aQueue.task_done()
            
        aQueue.task_done()
        
    def __parserWorker(self, aQueue, aStopToken):    
        config = MyConfig()
        while True:
            item = aQueue.get()
                                                
            if item[1] == aStopToken:
                break
            
            newsitem = self.__getNewsItemByID(item[1][1][5])
            if newsitem == None:
                print("Parsing " + item[1][1][5])
                tse = None
                try:
                    tse = time.mktime(time.strptime(item[1][1][3], "%a, %d %b %Y %H:%M:%S %Z"))
                except:
                    pass
                try:
                    tse = time.mktime(time.strptime(item[1][1][3].replace(":", ""), "%Y-%m-%dT%H%M%S%z"))
                except:
                    pass
                        
                if self.intradayconnection.hasProperStart(item[1][1][1], tse) and tse != None:
                    for _ in range(3):
                        try:
                            nparticle = newspaper.article.Article(item[1][1][4], config=config)
                            nparticle.download(html=item[1][1][6])
                            nparticle.parse()
                            text = nparticle.text
                            
                            if text!= None and len(text) > 40 and item[1][1][1] in text:
                                rowvalues = (item[1][1][0], item[1][1][1], item[1][1][2], tse, item[1][1][4], item[1][1][5], text, None, None, None)
                                self.bowidfdbconnection.execute("INSERT INTO {0} VALUES (?,?,?,?,?,?,?,?,?,?)".format(self.articleTable), rowvalues, block=True)
                                toput = (item[1][0], (item[1][1][0], item[1][1][1], item[1][1][2], item[1][1][3], item[1][1][4], item[1][1][5], None))
                                if item[1][0] == self.predictLabel:
                                    self.predicterManager.put(toput)
                                elif item[1][0] == self.learnLabel:
                                    self.learnerManager.put(toput)
                            break
                    
                        except newspaper.article.ArticleException as e:
                            pass
                        except Exception as e:
                            pass
            else:
                toput = (item[1][0], (item[1][1][0], item[1][1][1], item[1][1][2], item[1][1][3], item[1][1][4], item[1][1][5], None))
                if item[1][0] == self.predictLabel:
                    self.predicterManager.put(toput)
                elif item[1][0] == self.learnLabel:
                    self.learnerManager.put(toput)
               
            aQueue.task_done() 

        aQueue.task_done()

    def __getNewsItemByID(self, aNewsID):
        newsitemlist = self.bowidfdbconnection.execute("SELECT * FROM {0} WHERE NewsID=? LIMIT 1".format(self.articleTable), (aNewsID,))
        if not newsitemlist:
            return None
        return newsitemlist[0]
    
    def __getTextInformation(self, text):
        textinformation = {}
        wordcounts = self.__tokenize(text)
        
        
        wordlist = list(wordcounts.keys())
        wordlist.append(self.masterWord)
        
        blocksize = 500
        for i in range(math.ceil(len(wordlist)/blocksize)):
            curparam = tuple(wordlist[i*blocksize:(i+1)*blocksize]) 
        
            sqlstatement = "SELECT * FROM {0} WHERE".format(self.wordTable) + " Word=? OR " * (len(curparam)-1) + " Word=? "
            
            for wordrow in self.bowidfdbconnection.execute(sqlstatement, curparam):
                if wordrow[0] == self.masterWord:
                    textinformation[wordrow[0]] = (0, wordrow)
                else:
                    textinformation[wordrow[0]] = (wordcounts[wordrow[0]], wordrow)
            
        
        for word in wordcounts:
            if word not in textinformation:
                textinformation[word] = (wordcounts[word], (word, 0, 0, self.__getEmptyPrediction().dumps()))
        
        if self.masterWord not in textinformation:
            textinformation[self.masterWord] = (0, (self.masterWord, 0, 0, self.__getEmptyPrediction().dumps()))
        
        return textinformation
        
    def tokenize(self, text):
        return self.__tokenize(text)
            
    def __tokenize(self, text):
        ret = {}
        
        textwot = re.sub("\([.]+:[.]+\)", " ", text, flags=re.DOTALL)
        wordlist = nltk.tokenize.word_tokenize(textwot)
        stemmer = nltk.stem.snowball.EnglishStemmer()
        stopwords = set( nltk.corpus.stopwords.words('english'))
        for word in wordlist:
            try:
                word = stemmer.stem(word).lower()
                if word and word not in stopwords and word.isalpha():
                    if word not in ret:
                        ret[word] = 0
                    ret[word] += 1
            except:
                pass
        return ret    
    
    def __moveLongLearn2Regular(self):
        try:
            while True:
                newsitem = self.longlearnqueue.get(False)
                self.learnerManager.put(newsitem)
        except queue.Empty:
            pass
   
    def __getEmptyPrediction(self):
        valtime = {}
        for i in range(39):
            valtime[i*30*60] = 0
        return ValueTimes(valtime)
    
class UpdateWord(CustomSQLFunction):
    def __init__(self, aTSSQLConnection):
        CustomSQLFunction.__init__(self, aTSSQLConnection, self.__intercept)
        
    def __intercept(self, dbcursor, sql, parameters):
        shouldintercept = False
        tokens = nltk.tokenize.word_tokenize(sql)
        if tokens[0].upper() == "UPDATEWORD":
            dbcursor.execute("""SELECT * 
                                                  FROM {0} 
                                                  WHERE Word=?
                                                  LIMIT 1""".format(BoWIDFIntelligence.wordTable),
                                                (parameters[0],))
            wordrow = dbcursor.fetchone()
            if wordrow != None:
                doccount = wordrow[1] + 1
                totalcount = wordrow[2] + parameters[2]
                if parameters[3] == None:
                    prediction = None
                else:
                    prediction = (((parameters[3] * parameters[2]) + (ValueTimes.loads(wordrow[3]) * wordrow[2])) * (1.0 / float(totalcount))).dumps()
                    
                dbcursor.execute("""UPDATE {0} 
                                            SET DocumentCount=?,
                                                TotalCount=?,
                                                Prediction=?
                                            WHERE Word=?""".format(BoWIDFIntelligence.wordTable), 
                                        (doccount, totalcount, prediction, parameters[0],))
                
            else:
                if parameters[3] == None:
                    prediction = None
                elif isinstance(parameters[3], str):
                    prediction = parameters[3]
                elif isinstance(parameters[3], ValueTimes):
                    prediction = parameters[3].dumps()
                else:
                    prediction = None
                    
                dbcursor.execute("""INSERT INTO {0} 
                                            VALUES (?,?,?,?)""".format(BoWIDFIntelligence.wordTable), 
                                        (parameters[0], 1, parameters[2], prediction,))
            
            shouldintercept = True
            
        return shouldintercept

class UpdateWords(CustomSQLFunction):
    def __init__(self, aTSSQLConnection):
        CustomSQLFunction.__init__(self, aTSSQLConnection, self.__intercept)
        
    def __intercept(self, dbcursor, sql, parameters):
        shouldintercept = False
        tokens = nltk.tokenize.word_tokenize(sql)
        if tokens[0].upper() == "UPDATEWORDS":
            for word in parameters[0]:
                dbcursor.execute("""SELECT * FROM {0} WHERE Word=? LIMIT 1""".format(BoWIDFIntelligence.wordTable), (word,))
                wordrow = dbcursor.fetchone()
                if wordrow != None:
                    doccount = wordrow[1] + 1
                    totalcount = wordrow[2] + parameters[0][word]
                    if parameters[1] == None:
                        prediction = None
                    else:
                        prediction = (((parameters[1] * parameters[0][word]) + (ValueTimes.loads(wordrow[3]) * wordrow[2])) * (1.0 / float(totalcount))).dumps()
                        
                    dbcursor.execute("""UPDATE {0} 
                                                SET DocumentCount=?,
                                                    TotalCount=?,
                                                    Prediction=?
                                                WHERE Word=?""".format(BoWIDFIntelligence.wordTable), 
                                            (doccount, totalcount, prediction, word,))
                    
                else:
                    if parameters[1] == None:
                        prediction = None
                    elif isinstance(parameters[1], str):
                        prediction = parameters[1]
                    elif isinstance(parameters[1], ValueTimes):
                        prediction = parameters[1].dumps()
                    else:
                        prediction = None
                        
                    dbcursor.execute("""INSERT INTO {0} 
                                                VALUES (?,?,?,?)""".format(BoWIDFIntelligence.wordTable), 
                                            (word, 1, parameters[0][word], prediction,))
                    
            dbcursor.execute("""INSERT OR IGNORE INTO {0} VALUES (?,?,?,?)""".format(BoWIDFIntelligence.wordTable), (BoWIDFIntelligence.masterWord,0,0,None))
            dbcursor.execute("""UPDATE {0} SET DocumentCount=DocumentCount+1 WHERE Word=?""".format(BoWIDFIntelligence.wordTable), (BoWIDFIntelligence.masterWord,))
            
            shouldintercept = True
            
        return shouldintercept
                    
class UpdateArticle(CustomSQLFunction):
    def __init__(self, aTSSQLConnection):
        CustomSQLFunction.__init__(self, aTSSQLConnection, self.__intercept)
        
    def __intercept(self, dbcursor, sql, parameters):
        shouldintercept = False
        if sql.upper().strip().startswith("UPDATEARTICLE"):
            
            dbcursor.execute("""SELECT MAX(PCount) FROM {0}""".format(BoWIDFIntelligence.articleTable))
            wordrow = dbcursor.fetchone()
            if wordrow == None:
                maxvalue = 0
            elif wordrow[0] == None:
                maxvalue = 0
            else:
                maxvalue = wordrow[0]

            if isinstance(parameters[0], str):
                prediction = parameters[0]
            elif isinstance(parameters[0], ValueTimes):
                prediction = parameters[0].dumps()
            else:
                prediction = None
                
            dbcursor.execute("""UPDATE {0} SET Prediction=?, PCount=? WHERE NewsID=?""".format(BoWIDFIntelligence.articleTable), (prediction, maxvalue+1, parameters[1],))
    
            shouldintercept = True
            
        return shouldintercept
            