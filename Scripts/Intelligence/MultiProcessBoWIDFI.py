'''
Created on Nov 2, 2015

@author: Sean
'''

from math import log, ceil
import time

import newspaper

import BoWIDFI
import Dictionary
import Helpers
import Logger
import re
import nltk
import queue

from multiprocessing import pool
import multiprocessing
import threading


module_logging = Logger.getLogger(__name__)
exception_module_logging = Logger.getSecondaryExceptionLogger(__name__)

class BoWIDFIntelligence(BoWIDFI.BoWIDFIntelligence):
    def __init__(self, aCompanyInformationList, aBoWIDFDatabaseName, aIDFinanceClientIntelligent):
        self.status = "OPEN"

        self.companyinformationlist = aCompanyInformationList

        self.bowidfdbconnection = Helpers.TSSQLConnection(aBoWIDFDatabaseName)
        BoWIDFI.UpdateWord(self.bowidfdbconnection)
        BoWIDFI.UpdateArticle(self.bowidfdbconnection)
        BoWIDFI.UpdateWords(self.bowidfdbconnection)

        self.__createTables()
        self.intradayconnection = aIDFinanceClientIntelligent

        self.dictionary = Dictionary.Dictionary()

        self.parsepool = pool.Pool(processes=4)
        
        self.PaLQueue = queue.Queue()
        self.StopToken = "STOP"
        self.longlearnqueue = queue.Queue()

        self.workerThread = threading.Thread(target=self.__PaLWorker, name="IntelligentWorker")
        self.workerThread.start()
        
        self.poolLock = threading.RLock()
        
    def predict(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource):
        args = (aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, self.predictLabel)
        self.PaLQueue.put({"Given":args})
        
    def learn(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource):
        args = (aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, self.learnLabel)
        self.PaLQueue.put({"Given":args})
        
    def __predict(self, aService, aTicker, aTitle, aPubDate, aLink, aNewsID, fromFunc, wordcounts):
        textinfo = self.__getTextInformation(wordcounts)
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
                    mastervaltime = mastervaltime + valtime*((128.0 * weights[word]) / weighttotal)
            mastervaltime = mastervaltime * (1 / 128.0)
        self.bowidfdbconnection.execute("UPDATEARTICLE {0} SET Prediction=? WHERE NewsID=?".format(self.articleTable), (mastervaltime, aNewsID,))
            
    def __learn(self, aService, aTicker, aTitle, aPubDateTSE, aLink, aNewsID, fromFunc, wordcounts):
        data = self.intradayconnection.getData(aTicker, aPubDateTSE, increment=BoWIDFIntelligence.intradayincrement, count=BoWIDFIntelligence.intradaycount)
        if data == None:
            self.longlearnqueue.put((aService, aTicker, aTitle, aPubDateTSE, aLink, aNewsID, None, fromFunc))
        else:
            self.bowidfdbconnection.execute("UPDATEWORDS", (wordcounts, data))
            self.bowidfdbconnection.execute("UPDATEARTICLE {0} SET Actual=? WHERE NewsID=?".format(self.articleTable), (data, aNewsID,))
            
    def __getTextInformation(self, wordcounts):
        textinformation = {}
        
        wordlist = list(wordcounts.keys())
        wordlist.append(self.masterWord)
        
        blocksize = 500
        for i in range(ceil(len(wordlist)/blocksize)):
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
        
    def __PaLWorker(self):
        while True:
            aitem = self.PaLQueue.get()
            if aitem == self.StopToken:
                break
            newsitem = self.__getNewsItemByID(aitem["Given"][5])
            if newsitem == None:
                
                tse = pubdate2TSE(aitem["Given"][3])
                avalidstart = self.intradayconnection.hasProperStart(aitem["Given"][1], tse)
                
                arglist = list(aitem["Given"])
                arglist.append(avalidstart)
                arglist.append(self.dictionary)
                with self.poolLock:
                    self.parsepool.apply_async(doParse, args=tuple(arglist), callback=self.__PTCallback)
                
            elif "Text" in aitem or "TokenCounts" in aitem:
                if "Text" in aitem:
                    rowvalues = (aitem["Given"][0], aitem["Given"][1], aitem["Given"][2], aitem["Given"][3], aitem["Given"][4], aitem["Given"][5], aitem["Text"], None, None, None, None)
                    self.bowidfdbconnection.execute("INSERT INTO {0} VALUES (?,?,?,?,?,?,?,?,?,?,?)".format(self.articleTable), rowvalues)
                    
                if "TokenCounts" in aitem:
                    if aitem["Given"][6] == self.predictLabel:
                        if (newsitem[7] == None or len(newsitem[7]) < 5):
                            self.__predict(aitem["Given"][0], aitem["Given"][1], aitem["Given"][2], aitem["Given"][3], aitem["Given"][4], aitem["Given"][5], aitem["Given"][6], aitem["TokenCounts"])
                    elif aitem["Given"][6] == self.learnLabel:
                        if (newsitem[7] == None or len(newsitem[7]) < 5):
                            self.__predict(aitem["Given"][0], aitem["Given"][1], aitem["Given"][2], aitem["Given"][3], aitem["Given"][4], aitem["Given"][5], aitem["Given"][6], aitem["TokenCounts"])
                        if newsitem[8] == None:
                            self.__learn(aitem["Given"][0], aitem["Given"][1], aitem["Given"][2], aitem["Given"][3], aitem["Given"][4], aitem["Given"][5], aitem["Given"][6], aitem["TokenCounts"])

            elif (newsitem[7] == None or len(newsitem[7]) < 5) or newsitem[8] == None:
                arglist = list(aitem["Given"])
                arglist.append(newsitem[6])
                arglist.append(self.dictionary)
                with self.poolLock:
                    self.parsepool.apply_async(doTokenize, args=tuple(arglist), callback=self.__PTCallback)
                
                
            self.PaLQueue.task_done()
            
        self.PaLQueue.task_done()        
    
    def __PTCallback(self, aValue):
        if aValue:
            self.PaLQueue.put(aValue)
         
         
    def join(self):
        self.PaLQueue.join()
        self.PaLQueue.join()
        self.PaLQueue.join()
        with self.poolLock:
            self.parsepool.close()
            self.parsepool.join()
            print("Joining")
            self.parsepool = pool.Pool(processes=4)
        self.PaLQueue.join()
        self.PaLQueue.join()
        self.PaLQueue.join()

        
    def close(self):
        if self.status == "OPEN":
            self.status = "CLOSED" 
            self.join()
            self.__LLQueue2Table() 
            with self.poolLock:
                self.parsepool.close()
                self.parsepool.join()
                self.parsepool = None
                
            self.PaLQueue.put(self.StopToken)
            self.workerThread.join()
            self.bowidfdbconnection.close()
            
             
    def __del__(self):
        self.close()
            
def pubdate2TSE(aDateString):
    tse = None
    try:
        tse = time.mktime(time.strptime(aDateString, "%a, %d %b %Y %H:%M:%S %Z"))
    except:
        pass
    try:
        tse = time.mktime(time.strptime(aDateString.replace(":", ""), "%Y-%m-%dT%H%M%S%z"))
    except:
        pass
    return tse

def doParse(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc, aValidStart, aDictionary):
    tse = pubdate2TSE(aPubDate)
    if aValidStart and tse != None:
        for _ in range(1):
            try:
                nparticle = newspaper.article.Article(aLink, config=Helpers.MyConfig())
                nparticle.download(html=aFullSource)
                nparticle.parse()
                text = nparticle.text
                
                if text!= None and len(text) > 40 and aTicker in text:
                    retdict = doTokenize(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, None, fromFunc, text, aDictionary) 
                    retdict["Text"] = text
                    return retdict
                break
        
            except Exception as e:
                exception_module_logging.exception(e)
    return None
    
def doTokenize(aService, aTicker, aTitle, aPubDate, aLink, aNewsID, aFullSource, fromFunc, aText, aDictionary):
    tse = pubdate2TSE(aPubDate)
    rettokens = {}
    textwot = re.sub("\([.]+:[.]+\)", " ", aText, flags=re.DOTALL)
    wordlist = nltk.tokenize.word_tokenize(textwot)
    stemmer = nltk.stem.snowball.EnglishStemmer()
    for word in wordlist:
        try:
            if (word in aDictionary):
                stemword = stemmer.stem(word).lower()
                if stemword not in rettokens:
                    rettokens[stemword] = 0
                rettokens[stemword] += 1
        except Exception as e:
            exception_module_logging.exception(e)
    return {"TokenCounts":rettokens, "Given":(aService, aTicker, aTitle, tse, aLink, aNewsID, fromFunc)}    
    