'''
Created on Nov 19, 2015

@author: Sean
'''

import nltk
from builtins import str

class Dictionary:
    macstopwordsfilename = "/usr/share/dict/connectives"
    macdictionaryfilename = "/usr/share/dict/words"
    
    def  __init__(self):
        nltkstopwords = set(nltk.corpus.stopwords.words('english'))
        macstopwords = set()
        with open(self.macstopwordsfilename, 'r') as macstopwordsfile:
            for line in macstopwordsfile:
                macstopwords.add(line.strip().lower())
        
        self.stopwords = nltkstopwords.union(macstopwords)
        
        self.dictionarywords = set()
        with open(self.macdictionaryfilename, 'r') as macdictionaryfile:
            for line in macdictionaryfile:
                self.dictionarywords.add(line.strip().lower())
                
                
    def __contains__(self, item):
        if isinstance(item, str):
            return (item.lower() in self.dictionarywords) and (item.lower() not in self.stopwords)
        return False