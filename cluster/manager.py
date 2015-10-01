"""
	manager.py 
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

import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from scipy import stats

class Manager:

	def __init__(self):
		pass

	def loadDataFromFile(self, fileName="genHistory.csv"):
		print "Loading data..."
		answerList = ( # = (  dateList, openList, highList , lowList, closeList, volumeList, adjCloseList
			np.genfromtxt(fileName, dtype=None, delimiter=',', skiprows=1, usecols=(0,1,2,3,4,5,6)) # unpack=True
		)
		#for x in range (2):
		#	print answerList[x] #+ " " + openList[x]
		
		print "Splitting raw data..."
		dateList = np.asarray([datetime.strptime(x[0], "%m/%d/%Y") for x in answerList])
		openList = np.asarray([x[1] for x in answerList])
		#closeList = [x[3] for x in answerList]  
		#openList, highList, lowList, closeList, volumeList, adjCloseList = 

		print "Normalizing data by Z-score..."
		z_openList = stats.zscore(openList)

		print "Splitting into small periods..."
		miniStockList = []
		for i, miniStock in enumerate(self.chunks(z_openList, 100)):
			miniStockList.append(miniStock)

		print "Clustering..."
		print "TODO"

		print "Graphing data..."
		for i in range(5):
			plt.subplot(2,5,i+1)
			plt.ylabel(str(i))
			plt.plot(dateList[:len(miniStockList[i])], miniStockList[i], color='r')
		plt.subplot(256)
		plt.plot(dateList, openList, color='m')
		plt.subplot(257)
		plt.plot(dateList, z_openList, color='c')
		plt.show()


	def chunks(self, l, n):
	    num = len(l)/n
	    answer = []
	    for i in xrange(0, len(l), num):
	        answer.append(l[i:i+num])
	    return answer

	def generateClustersTemp(self):
		raiseNotImplemented()

	def raiseNotImplemented(self):
		raise RuntimeError("ERROR: not yet implemented.")

