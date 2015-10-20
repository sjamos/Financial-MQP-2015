"""
	controller.py 
	author: Nicholas S. Bradford

	Class for assembling a portfolio.
	
	-gather data
		-load from .csv files
	-cluster stocks (most likely 10)
		-preprocessing: de-trending, normalize by z-score
			detrended value = closing price - avg. closing price
			Zscore = detrended value / std. deviation of closing price
		-use elbow method to determine optimal number of clusters
		-perform clustering, store cluster results and cluster prototypes
	-train NN on each cluster

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

	def __init__(self):
		pass

	@staticmethod
	def normalize(data_list):
		"""	Z-score the first 4 elements together, and the 5th element separately.
			Note: volume (element 5) is currently being dropped.
		"""
		noVolumeList = [x[:4] for x in data_list]
		zList = stats.zscore(noVolumeList, axis=None)
		return zList

	@staticmethod
	def getTargets(data):
		"""	data stored as (open, high, low, close, volume, price).
			currently no volume or price.
		"""
		target = []
		for i in range(len(data)):
			if i == 0 or i == 1:
				continue;
			row = data[i];
			t = [0];
			if row[3] > 0: # row[0] if the close is higher than 0, which is the normalized open
				t[0] = 1;		
			target.append(t); # list with one element, one for high, or zero for low

		assert len(data) == len(target) + 2, "ERROR: data and target must have same length."
		for day in data:
			assert len(day) == 4, "ERROR: day has " + str(len(day)) + " elements instead of 4."
		return target

	@staticmethod
	def get_SP500():
		stocks_SP500 = np.genfromtxt('constituents.csv', dtype=None, delimiter=',', skiprows=1, usecols=(0))
		return stocks_SP500

	@staticmethod
	def getStockDataList(stock_list, training_time):
		stock_data_list = []
		ticker_list = []
		for ticker in stock_list:
			try:
				raw_data = Manager.loadTrainingData(training_time, ticker)
				normalized_data = raw_data #Manager.normalize(raw_data)
				#target_list.append(Manager.getTargets(normalized_data))
				if len(normalized_data) == 252:
					ticker_list.append(ticker)
					stock_data_list.append(normalized_data[:-2]) 	# delete last data entry, because it won't be used
				else:
					print "Stock Error: Contained", len(normalized_data), "instead of 252."
			except IOError:
				print "IOError: Could not fetch", ticker, "from Yahoo! Finance."
		return ticker_list, stock_data_list

	#==============================================================================================

	@staticmethod
	def loadTrainingData(training_time, training_stock, IS_NORMALIZE=True):
		"""	Data stored as (open, high, low, close, volume, price)
			Only take adjusted (open, high, low, close)
		"""
		answer = Manager.loadData(2013, 2013+training_time, stock_list=[training_stock])
		answer = Manager.loadConvertDataFormat(answer)
		# choose whether or not to normalize
		if IS_NORMALIZE:
			answer = ([ 							
						([	x[0] - x[0], 	# open
							x[1] - x[0],	# high 	- open
							x[2] - x[0],	# low 	- open
							x[3] - x[0],	# close - open
							#data[context.security].volume,
							#data[context.security].close,
						]) for x in answer
			])
		else:
			answer = ([ 							
						([	x[0], 	# open
							x[1],	# high 	
							x[2],	# low 	
							x[3],	# close 
							#data[context.security].volume,
							#data[context.security].close,
						]) for x in answer
			])
		return answer


	#==============================================================================================

	@staticmethod
	def loadData(startYear, endYear, stock_list, startM=1, endM=1):
		"""	Load data, stored as (open, high, low, close, volume, price).
			Must convert pandas.Panel --> pandas.DataFrame --> List of Lists
		"""
		start = datetime(startYear, startM, 1, 0, 0, 0, 0, pytz.utc)
		end = datetime(endYear, endM, 1, 0, 0, 0, 0, pytz.utc) 
		data = load_bars_from_yahoo(stocks=stock_list, 
									start=start,
									end=end)
		return data

	#==============================================================================================

	@staticmethod
	def loadConvertDataFormat(data):
		answer = data.transpose(2, 1, 0, copy=True).to_frame()	# pandas.Panel --> pandas.DataFrame
		answer = answer.values.tolist() 						# pandas.DataFrame --> List of Lists
		return answer



