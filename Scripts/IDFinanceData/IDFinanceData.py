'''
Created on Oct 26, 2015

@author: Sean
'''
import time


class IDFinanceData(object):
    '''
    classdocs
    '''


    def __init__(self, aTicker, aURL, aSession, aService, aCleanDataFunction):
        '''
        Constructor
        '''
        self.ticker = aTicker
        self.url = aURL
        self.session = aSession
        self.service = aService
        self.cleandata = aCleanDataFunction
        time.sleep(0.33)
        self.actsession = self.session.get(self.url, timeout=(5, 30))
        self.data = None
        
    def setData(self):
        if(self.data == None):
            try:
                resp = self.actsession.result()
                if resp.status_code == 200:           
                    self.data = self.cleandata(resp.text)
                else:
                    time.sleep(0.33)
                    self.actsession = self.session.get(self.url, timeout=(5, 30))
            except:
                time.sleep(0.33)
                self.actsession = self.session.get(self.url, timeout=(5, 30))
        return self.data