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

#==============================================================================================

class Manager:

	def __init__(self):
		pass

	@staticmethod
	def normalizeByZScore(dataList):
		"""	Normalizes a list by Z-Score.
			Parameters:
				dataList (list of float): data to normalize.
			Returns:
				The normalized list of data, as an nparray.
		"""
		return stats.zscore(dataList)

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
			if row[3] > 0: # if the close is higher than 0, which is the normalized open
				t[0] = 1;		
			target.append(t); # list with one element, one for high, or zero for low

		assert len(data) == len(target) + 2, "ERROR: data and target must have same length."
		for day in data:
			assert len(day) == 4, "ERROR: day has " + str(len(day)) + " elements instead of 4."
		return target

	@staticmethod
	def normalize(data_list):
		"""	Z-score the first 4 elements together, and the 5th element separately.
			Note: volume (element 5) is currently being dropped.
		"""
		noVolumeList = [x[:4] for x in data_list]
		zList = stats.zscore(noVolumeList, axis=None)
		return zList
		