'''
Created on Nov 2, 2015

@author: Sean
'''

import queue
import re
from math import log
import time
import newspaper
import nltk
import math
import Logger
import Helpers 
import Dictionary


module_logging = Logger.getLogger(__name__)
exception_module_logging = Logger.getSecondaryExceptionLogger(__name__)

class BoWIDFIntelligence:
    masterWord = "__MASTER_WORD__"
        
    wordTable = "Words"
    articleTable = "Articles"
    predictQueueTable = "ToPredictQueue"
    learnQueueTable = "ToLearnQueue"
    parseQueueTable = "ToParseQueue"
    curPredictionTable = "CurrentPredictions"
    
    predictLabel = "PREDICT"
    learnLabel = "LEARN"
    parseLabel = "PARSE"
    
    intradayincrement = 10
    intradaycount = 100
    
    
    def __init__(self, aCompanyInformationList, aBoWIDFDatabaseName, aIDFinanceClientIntelligent,
                         LearningThreads=3, PredictingThreads=2, ParsingThreads=4):
        self.status = "OPEN"
        
        self.companyinformationlist = aCompanyInformationList
        
        self.bowidfdbconnection = Helpers.SQLConnection(aBoWIDFDatabaseName)
        UpdateWord(self.bowidfdbconnection)
        UpdateArticle(self.bowidfdbconnection)
        UpdateWords(self.bowidfdbconnection)
        self.__createTables()
        self.intradayconnection = aIDFinanceClientIntelligent
        self.longlearnqueue = queue.Queue()
        self.dictionary = Dictionary.Dictionary()
        
    def __createTables(self):
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                Word text UNIQUE, 
                                DocumentCount int, 
                                TotalCount int, 
                                Prediction text)""".format(self.wordTable))
        
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
                                        PCount int,
                                        LCount int)""".format(self.articleTable))
                
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                        Service text,
                                        Ticker text,
                                        Title text,
                                        PubDate text,
                                        Link text,
                                        NewsID text UNIQUE,
                                        FullContent text,
                                        Caller text)""".format(self.predictQueueTable))
        
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                        Service text,
                                        Ticker text,
                                        Title text,
                                        PubDate text,
                                        Link text,
                                        NewsID text UNIQUE,
                                        FullContent text,
                                        Caller text)""".format(self.learnQueueTable))
        
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                        Service text,
                                        Ticker text,
                                        Title text,
                                        PubDate text,
                                        Link text,
                                        NewsID text UNIQUE,
                                        FullContent text,
                                        Caller text)""".format(self.parseQueueTable))
        
        self.bowidfdbconnection.execute("""CREATE TABLE IF NOT EXISTS {0}(
                                        NewsID text UNIQUE,
                                        CurrentPrediction text,
                                        LCount int)""".format(self.curPredictionTable))
        
    def predict(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc=None):
        newsitem = self.__getNewsItemByID(aNewsID)
        
        if newsitem == None:
            self.__parse(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc=self.predictLabel)
            
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
                    valtime = Helpers.ValueTimes.loads(textinfo[word][1][3])
                    if weights[word] > 0 and not valtime.isZero():
                        mastervaltime = mastervaltime + valtime*(weights[word] / weighttotal)
                        
            self.bowidfdbconnection.execute("UPDATEARTICLE {0} SET Prediction=? WHERE NewsID=?".format(self.articleTable), (mastervaltime, aNewsID,))
            
        if fromFunc == self.learnLabel:
            self.learn(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc=fromFunc)

    def learn(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc=None, skipPrediction=False):
        newsitem = self.__getNewsItemByID(aNewsID)
        if newsitem == None:
            self.__parse(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc=self.learnLabel) 
        elif (newsitem[7] == None or len(newsitem[7]) < 5) and not skipPrediction:
            self.predict(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc=self.learnLabel)
        elif newsitem[8] == None:
            data = self.intradayconnection.getData(aTicker, newsitem[3], increment=BoWIDFIntelligence.intradayincrement, count=BoWIDFIntelligence.intradaycount)
            if data == None:
                self.longlearnqueue.put((aService, aTicker, aTitle, aPubDate, aLink, aNewsID, None, fromFunc))
            else:
                tokens = self.__tokenize(newsitem[6])
                self.bowidfdbconnection.execute("UPDATEWORDS", (tokens, data))
                self.bowidfdbconnection.execute("UPDATEARTICLE {0} SET Actual=? WHERE NewsID=?".format(self.articleTable), (data, aNewsID,))
                     
    def __parse(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc):
        newsitem = self.__getNewsItemByID(aNewsID)
        if newsitem == None:
            tse = None
            try:
                tse = time.mktime(time.strptime(aPubDate, "%a, %d %b %Y %H:%M:%S %Z"))
            except:
                pass
            try:
                tse = time.mktime(time.strptime(aPubDate.replace(":", ""), "%Y-%m-%dT%H%M%S%z"))
            except:
                pass
                    
            if self.intradayconnection.hasProperStart(aTicker, tse) and tse != None:
                for _ in range(1):
                    try:
                        nparticle = newspaper.article.Article(aLink, config=Helpers.MyConfig())
                        nparticle.download(html=aFullSource)
                        nparticle.parse()
                        text = nparticle.text
                        
                        if text!= None and len(text) > 40 and aTicker in text:
                            rowvalues = (aService, aTicker, aTitle, tse, aLink, aNewsID, text, None, None, None, None)
                            self.bowidfdbconnection.execute("INSERT INTO {0} VALUES (?,?,?,?,?,?,?,?,?,?,?)".format(self.articleTable), rowvalues)
                            if fromFunc == self.predictLabel:
                                self.predict(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc)
                            elif fromFunc == self.learnLabel:
                                self.learn(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc)
                        break
                
                    except Exception as e:
                        exception_module_logging.exception(e)
                    
        else:
            if fromFunc == self.predictLabel:
                self.predict(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc)
            elif fromFunc == self.learnLabel:
                self.learn(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc)
          
    def close(self):
        if(self.status != "CLOSED"):
            self.status = "CLOSED"
            
            self.__LLQueue2Table()
            
            self.bowidfdbconnection.close()

             
    def __del__(self):
        self.close()
    
    def __LLQueue2Table(self):
        try:
            while True:
                item = self.longlearnqueue.get(False)
                toput = (item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7],)
                self.bowidfdbconnection.execute("INSERT OR IGNORE INTO {0} VALUES (?,?,?,?,?,?,?,?)".format(self.learnQueueTable), toput)
                self.longlearnqueue.task_done()
        except queue.Empty:
            pass
        
    def __Table2LQueue(self):
        rows = self.bowidfdbconnection.execute("SELECT * FROM {0};".format(self.learnQueueTable))
        for row in rows:
            toput = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
            self.longlearnqueue.put(toput)
        self.bowidfdbconnection.execute("DELETE FROM {0};".format(self.learnQueueTable))
        self.bowidfdbconnection.execute("VACUUM;")
        self.__moveLongLearn2Regular()

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
        for word in wordlist:
            try:
                if (word in self.dictionary):
                    stemword = stemmer.stem(word).lower()
                    if stemword not in ret:
                        ret[stemword] = 0
                    ret[stemword] += 1
            except Exception as e:
                exception_module_logging.exception(e)
        return ret    
    
    def __moveLongLearn2Regular(self):
        try:
            while True:
                newsitem = self.longlearnqueue.get()
                self.learn(newsitem[0], newsitem[1], newsitem[2], newsitem[3], newsitem[4], newsitem[5], newsitem[6], newsitem[7])
        except queue.Empty:
            pass
   
    def __getEmptyPrediction(self):
        valtime = {}
        for i in range(BoWIDFIntelligence.intradaycount):
            valtime[i*60*BoWIDFIntelligence.intradayincrement] = 0
        return Helpers.ValueTimes(valtime)
    
class UpdateWord(Helpers.CustomSQLFunction):
    def __init__(self, aTSSQLConnection):
        super().__init__(aTSSQLConnection, self.__intercept)
        
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
                    prediction = (((parameters[3] * parameters[2]) + (Helpers.ValueTimes.loads(wordrow[3]) * wordrow[2])) * (1.0 / float(totalcount))).dumps()
                    
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
                elif isinstance(parameters[3], Helpers.ValueTimes):
                    prediction = parameters[3].dumps()
                else:
                    prediction = None
                    
                dbcursor.execute("""INSERT INTO {0} 
                                            VALUES (?,?,?,?)""".format(BoWIDFIntelligence.wordTable), 
                                        (parameters[0], 1, parameters[2], prediction,))
            
            shouldintercept = True
            
        return (shouldintercept, None)

class UpdateWords(Helpers.CustomSQLFunction):
    def __init__(self, aTSSQLConnection):
        super().__init__(aTSSQLConnection, self.__intercept)
        
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
                        #prediction = (((parameters[1] * parameters[0][word]) + (Helpers.ValueTimes.loads(wordrow[3]) * wordrow[2])) * (1.0 / float(totalcount))).dumps()
                        prediction = (((parameters[1] * 1) + (Helpers.ValueTimes.loads(wordrow[3]) * wordrow[1])) * (1.0 / float(doccount))).dumps()
                        
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
                    elif isinstance(parameters[1], Helpers.ValueTimes):
                        prediction = parameters[1].dumps()
                    else:
                        prediction = None
                        
                    dbcursor.execute("""INSERT INTO {0} 
                                                VALUES (?,?,?,?)""".format(BoWIDFIntelligence.wordTable), 
                                            (word, 1, parameters[0][word], prediction,))
                    
            dbcursor.execute("""INSERT OR IGNORE INTO {0} VALUES (?,?,?,?)""".format(BoWIDFIntelligence.wordTable), (BoWIDFIntelligence.masterWord,0,0,None))
            dbcursor.execute("""UPDATE {0} SET DocumentCount=DocumentCount+1 WHERE Word=?""".format(BoWIDFIntelligence.wordTable), (BoWIDFIntelligence.masterWord,))
            
            shouldintercept = True
            
        return (shouldintercept, None)
                    
class UpdateArticle(Helpers.CustomSQLFunction):
    def __init__(self, aTSSQLConnection):
        super().__init__(aTSSQLConnection, self.__intercept)
        
    def __intercept(self, dbcursor, sql, parameters):
        shouldintercept = False
        tokens = sql.replace(",", " ").split()
        if tokens[0].upper() == "UPDATEARTICLE":
            if "CurrentPrediction=?" in tokens:
                if isinstance(parameters[0], str):
                    prediction = parameters[0]
                elif isinstance(parameters[0], Helpers.ValueTimes):
                    prediction = parameters[0].dumps()
                else:
                    prediction = None
                
                dbcursor.execute("""INSERT OR IGNORE INTO {0} VALUES(?,?,?)""".format(tokens[1]), (parameters[1], prediction, parameters[2],))
                dbcursor.execute("""UPDATE {0} SET CurrentPrediction=?, LCount=? WHERE NewsID=?""".format(tokens[1]), (prediction, parameters[2], parameters[1],))
                shouldintercept = True
                
            elif "Prediction=?" in tokens:
                dbcursor.execute("""SELECT MAX(PCount) FROM {0}""".format(tokens[1]))
                wordrow = dbcursor.fetchone()
                if wordrow == None:
                    maxvalue = 0
                elif wordrow[0] == None:
                    maxvalue = 0
                else:
                    maxvalue = wordrow[0]
    
                if isinstance(parameters[0], str):
                    prediction = parameters[0]
                elif isinstance(parameters[0], Helpers.ValueTimes):
                    prediction = parameters[0].dumps()
                else:
                    prediction = None
                    
                dbcursor.execute("""UPDATE {0} SET Prediction=?, PCount=? WHERE NewsID=?""".format(tokens[1]), (prediction, maxvalue+1, parameters[1],))
        
                shouldintercept = True
                
            elif "Actual=?" in tokens:
                dbcursor.execute("""SELECT MAX(LCount) FROM {0}""".format(tokens[1]))
                wordrow = dbcursor.fetchone()
                if wordrow == None:
                    maxvalue = 0
                elif wordrow[0] == None:
                    maxvalue = 0
                else:
                    maxvalue = wordrow[0]
    
                if isinstance(parameters[0], str):
                    prediction = parameters[0]
                elif isinstance(parameters[0], Helpers.ValueTimes):
                    prediction = parameters[0].dumps()
                else:
                    prediction = None
                    
                dbcursor.execute("""UPDATE {0} SET Actual=?, LCount=? WHERE NewsID=?""".format(tokens[1]), (prediction, maxvalue+1, parameters[1],))
        
                shouldintercept = True
            
        return (shouldintercept, None)
    