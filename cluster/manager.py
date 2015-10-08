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

from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import collections as mc

from scipy import stats
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import pylab as pl

class Manager:

	def __init__(self):
		pass

	def loadDataFromFile(self, fileName):
		"""	Reads in data from a .csv file.
			Parameters:
				fileName (str): name of the file to read in data from.
			Returns:
				A list of lists, which each represent a column from the data read.
		"""
		# dateList, openList, highList, lowList, closeList, volumeList, adjCloseList 
		# unpack=True
		return np.genfromtxt(fileName, dtype=None, delimiter=',', skiprows=1, usecols=(0,1,2,3,4,5,6))

	def normalizeByZScore(self, dataList):
		"""	Normalizes a list by Z-Score.
			Parameters:
				dataList (list of float): data to normalize.
			Returns:
				The normalized list of data, as an nparray.
		"""
		return stats.zscore(dataList)

	def graphClusters(self, clusters, dateList):
		"""
			Parameters:
				clusters (list): list of list(cluster) of list(stock) of doubles
				dateList (list): list of dates 
		"""
		plt.figure("Clusters")
		for i, cluster in enumerate(clusters):
			for stock in clusters[i]:
				assert len(stock) == len(dateList)
				plt.subplot(2,5,i+1)
				plt.ylabel("Cluster" + str(i))
				plt.plot(dateList, stock)

	def getXandY(self, fileName):
		rawData = self.loadDataFromFile(fileName) #dateList, openList, highList, lowList, closeList, volumeList, adjCloseList 
		data = [[ x[1], x[2]-x[1], x[3]-x[1], x[4]-x[1] ] for x in rawData] # normalize down
		
		target = []
		for i in range(len(data)):
			if (i == 0):
				continue;
			row = data[i];
			#print(row);
			t = [0];
			if row[2] > 0:
				t[0] = 1;		
			target.append(t); # list with one element, one for high, or zero for low
		
		#plt.figure("training data")
		#plt.plot([x[1] for x in data])
		#plt.show()

		del data[-1]; # delete last data entry, because it won't be used
		return (data, target)

	def run(self, fileName):
		print "Loading data..."
		answerList = self.loadDataFromFile(fileName)
		
		print "Splitting raw data..."
		dateList = [datetime.strptime(x[0], "%m/%d/%Y") for x in answerList]
		openList = [x[1] for x in answerList]

		print "Normalizing data by Z-score..."
		z_openList = stats.zscore(openList)

		print "Splitting into small periods..."
		miniStockList = []

		for i, miniStock in enumerate(self.chunks(openList, 57)):
			miniStockList.append(self.normalizeByZScore(miniStock))

		#--------------------------------------------------------------------------------
		
		print "Clustering into 10 groups..."
		kmeans = KMeans(n_clusters=10)
		kmeans.fit(miniStockList)

		clusters = {}
		for i, label in enumerate(kmeans.labels_):
			if label in clusters:
				clusters[label].append(miniStockList[i])
			else:
				clusters[label] = list()
				clusters[label].append(miniStockList[i])	
		
		print "# of Clusteres: " + str(len(clusters))
		print "# of stocks: " + str(len(miniStockList))
		print "Cluster sizes: "
		for key in clusters:
			print str(key) + ": " + str(len(clusters[key]))

		print "Transform using PCA..."
		pca = PCA(n_components=2).fit(miniStockList)
		pca_2d = pca.transform(miniStockList)
		#
		print "Graph reference..."
		#plt.figure('Reference Plot')
		#pl.scatter(pca_2d[:, 0], pca_2d[:, 1])

		"""
		print "Graph raw data..."
		for i in range(5):
			plt.subplot(2,5,i+1)
			plt.ylabel(str(i))
			plt.plot(dateList[:len(miniStockList[i])], miniStockList[i], color='r')
		plt.subplot(256)
		plt.plot(dateList, openList, color='m')
		plt.subplot(257)
		plt.plot(dateList, z_openList, color='c')
		"""

		print "Graph invidivual clusters..."
		self.graphClusters(clusters, dateList[:len(clusters[0][0])])
		
		#--------------------------------------------------------------------------------

		print "Graph clustering..."
		plt.figure("Clusters transformed to 2D")
		plt.subplot(1,2,1)
		plt.scatter(pca_2d[:, 0], pca_2d[:, 1], c=kmeans.labels_)

		print "Calculating for elbow method..."
		inertia = []
		for x in range(1, 31):
			kmeans = KMeans(n_clusters=x)
			kmeans.fit(miniStockList)
			inertia.append(kmeans.inertia_)
		plt.subplot(1,2,2)
		plt.plot(inertia)

		#--------------------------------------------------------------------------------

		print "Show..."
		plt.show()
		print "EXIT SUCCESS"

	def chunks(self, l, size):
	    #num = len(l)/n
	    answer = []
	    length = None
	    for i in xrange(0, len(l), size):
	        tmp = list(l[i:i+size])
	        if len(tmp) == size: answer.append(tmp)
	    return answer

	def generateClustersTemp(self):
		raiseNotImplemented()

	def raiseNotImplemented(self):
		raise RuntimeError("ERROR: not yet implemented.")


