"""
    manager.py 
    author: Nicholas S. Bradford

    Contains helpers.

"""

from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import collections as mc

from scipy import stats
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import pylab as pl

# handling dates
import pytz
from datetime import datetime

from zipline.utils.factory import load_bars_from_yahoo

#==============================================================================================


class Manager:

    @staticmethod
    def get_SP500():
        """ Return a list of tickers in the S&P 500."""
        stocks_SP500 = np.genfromtxt('manager/constituents.csv', dtype=None, delimiter=',', skiprows=1, usecols=(0))
        return stocks_SP500

    @staticmethod
    def get_DJIA():
        """ Return a list of tickers in the DJIA."""
        stocks_DJIA = ([
            'MMM', 
            'AXP', 
            'AAPL',
            'BA', 
            'CAT', 
            'CVX', 
            #'CSCS', 
            'KO', 
            'DIS', 
            'DD', 
            'XOM',
            'GE',
            #'G',
            'HD',
            'IBM',
            'INTC',
            'JNJ',
            'JPM',
            'MCD',
            'MRK',
            'MSFT',
            'MKE',
            'PFE',
            'PG',
            'TRV',
            'UTX',
            'UNH',
            'VZ',
            #'V',
            'WMT'
        ])
        return stocks_DJIA

    @staticmethod
    def getRawStockDataList(stock_list, training_start, training_end):
        """ Returns a list of tickers and list of stock data from Yahoo Finance."""
        ticker_list, stock_data_list = [], []
        for ticker in stock_list:
            try:
                raw_data = Manager.loadTrainingData(ticker, training_start, training_end)
                if len(raw_data) == 252:
                    ticker_list.append(ticker)
                    stock_data_list.append(raw_data)
                else:
                    print "Stock Error:", ticker, "contained", len(raw_data), "instead of 252."
            except IOError:
                print "Stock Error: could not load", ticker, "from Yahoo."  
        return ticker_list, stock_data_list

    @staticmethod
    def loadTrainingData(training_stock, training_start, training_end):
        """ Data stored as (open, high, low, close, volume, price)
            Only take adjusted (open, high, low, close)
        """
        start = datetime(training_start, 1, 1, 0, 0, 0, 0, pytz.utc)
        end = datetime(training_end, 1, 1, 0, 0, 0, 0, pytz.utc)
        data = load_bars_from_yahoo(stocks=[training_stock], start=start, end=end)
        data = Manager.convertPanelToList(data)
        data = ([                             
                    ([  x[0],   # open
                        x[1],   # high  
                        x[2],   # low   
                        x[3],   # close 
                        #x[4],  # volume
                        #x[5],  # price (same as close)
                    ]) for x in data # data stored as (open, high, low, close, volume, price)
        ])
        return data

    @staticmethod
    def convertPanelToList(data):
        """ Convert pandas.Panel --> pandas.DataFrame --> List of Lists """
        answer = data.transpose(2, 1, 0, copy=True).to_frame()
        answer = answer.values.tolist()
        return answer

    @staticmethod
    def preprocessData(stock_data, is_normalize=True):
        """ Takes in data for a single stock.
            Z-score the first 4 elements together, and the 5th element separately.
            Note: volume (element 5) is currently being dropped.
        """
        if not is_normalize:
            return stock_data
        else:
            #stock_data = [x[:4] for x in stock_data]
            zList = stats.zscore(stock_data, axis=None)
            return zList

