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

class Manager:

	def __init__(self):
		pass

	def loadDataFromFile(self, fileName="genHistory.csv"):
		print "Loading data..."
		answerList = ( # = (  dateList, openList, highList , lowList, closeList, volumeList, adjCloseList
			np.genfromtxt(fileName, dtype=None, delimiter=',', skiprows=1, usecols=(0,1,2,3,4,5,6)) # unpack=True
		)
		for x in range (2):
			print answerList[x] #+ " " + openList[x]
		
		dateList = [datetime.strptime(x[0], "%m/%d/%Y") for x in answerList] 
		openList = [x[1] for x in answerList] 
		closeList = [x[3] for x in answerList]  
		#openList, highList, lowList, closeList, volumeList, adjCloseList = 

		print "Graphing data..."
		plt.plot(dateList, openList, color='r')
		plt.plot(dateList, closeList, color='c')
		plt.show()

	def generateClustersTemp(self):
		raiseNotImplemented()

	def raiseNotImplemented(self):
		raise RuntimeError("ERROR: not yet implemented.")

