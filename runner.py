"""
	runner.py

	See README.md for usage.

	When creating a new class, you must ensure that it:
		-accepts a list of lists of stock data in its constructor (data, target, num_epochs)
		-has a predict() method
		-is added to the global strategy_dict{}

"""

import sys
import time
import argparse

# seed to prevent
import numpy as np
np.random.seed(13)

# clustering
from manager.manager import Manager
from sklearn.cluster import KMeans

# neural nets
from neuralnets.tradingnet import TradingNet
from neuralnets.deepnet import DeepNet, testNN

# backtesting
from zipline.api import order, record, symbol, history, add_history, get_open_orders, order_target_percent
from zipline.algorithm import TradingAlgorithm

# analysis
import matplotlib.pyplot as plt

# glabal strategy assigned in main(), along with the years to train, years to backtest, and epochs to train
global STRATEGY_CLASS, TRAINING_TIME, BACKTEST_TIME, EPOCHS, IS_NORMALIZE
global TRAINING_STOCK, BACKTEST_STOCK, SELECT_STOCKS # Training and backtest stock assigned in runMaster()

TRAINING_STOCK = 'SPY'
SELECT_STOCKS = ['AAPL', 'DIS', 'XOM', 'UNH', 'WMT']
strategy_dict = {
	1: TradingNet,
	4: DeepNet
}

#==============================================================================================

def initialize(context):
	"""	Define algorithm"""
	print "Initialize..."
	global TRAINING_STOCK, BACKTEST_STOCK
	context.security = None #becomes symbol(BACKTEST_STOCK)
	context.benchmark = symbol('SPY')
	
	context.training_data = Manager.loadTrainingData(TRAINING_TIME, TRAINING_STOCK)
	context.training_data_length = len(context.training_data) - 2
	context.normalized_data = Manager.normalize(context.training_data) 	# will have to redo every time step
	
	target = Manager.getTargets(context.normalized_data)
	context.training_data = context.training_data[:-2]			# delete last data entry, because it won't be used
	context.normalized_data = context.normalized_data[:-2] 		# delete last data entry, because it won't be used
	print target
	plt.figure("Training Data")
	for i in range(len(context.normalized_data[0])):
		plt.plot([x[i] for x in context.normalized_data])
	plt.legend(['open', 'high', 'low', 'close', 'volume', 'price'], loc='upper left')
	plt.show()

	print "Train..."
	#print len(context.training_data), len(context.normalized_data), len(target)
	context.strategy = STRATEGY_CLASS([context.normalized_data], [target], num_epochs=EPOCHS)
	
	print "Capital Base: " + str(context.portfolio.cash)

#==============================================================================================

# Gets called every time-step
def handle_data(context, data):
	#print "Cash: $" + str(context.portfolio.cash), "Data: ", str(len(context.training_data))
	#assert context.portfolio.cash > 0.0, "ERROR: negative context.portfolio.cash"
	assert len(context.training_data) == context.training_data_length; "ERROR: "
	context.security = symbol(BACKTEST_STOCK)

	# data stored as (open, high, low, close, volume, price)
	if IS_NORMALIZE:
		feed_data = ([	
						data[context.security].open 	- data[context.security].open, 
						data[context.security].high 	- data[context.security].open,
						data[context.security].low 		- data[context.security].open,
						data[context.security].close 	- data[context.security].open
						#data[context.security].volume,
						#data[context.security].close,
		])
	else:
		feed_data = ([	
						data[context.security].open, 
						data[context.security].high,
						data[context.security].low,
						data[context.security].close
						#data[context.security].volume,
						#data[context.security].close,
		])
	#keep track of history. 
	context.training_data.pop(0)
	context.training_data.append(feed_data)
	context.normalized_data = Manager.normalize(context.training_data) # will have to redo every time step
	#print len(context.training_data), len(context.normalized_data), len(context.normalized_data[0])

	prediction = context.strategy.predict(context.training_data)[-1]
	print "Value: $%.2f    Cash: $%.2f    Predict: %.5f" % (context.portfolio.portfolio_value, context.portfolio.cash, prediction[0])

	# Do nothing if there are open orders:
	if has_orders(context, data):
		print('has open orders - doing nothing!')
	# Put entire position in
	elif prediction > 0.5:
		order_target_percent(context.security, .98)
	# Take entire position out
	else:
		order_target_percent(context.security, 0)
		#order_target_percent(context.security, -.99)

	record(BENCH=data[context.security].price)
	record(SPY=data[context.benchmark].price)

#==============================================================================================

def has_orders(context, data):
	# Return true if there are pending orders.
	has_orders = False
	for stock in data:
		orders = get_open_orders(stock)
		if orders:
			return True

#==============================================================================================

def analyze(perf_list):
	""" To be called after the backtest. Will produce a .png in the /output/ directory."""
	print "Analyze..."
	for i, perf in enumerate(perf_list):
		plt.figure(i)
		plt.plot([x/perf.portfolio_value[1] for x in perf.portfolio_value[1:]])
		plt.plot([x/perf.BENCH[1] for x in perf.BENCH[1:]])
		plt.xlabel("Time (Days)")
		plt.ylabel("Percent Returns vs. SPY" + str(i))
		plt.legend(['algorithm', 'BENCHMARK'], loc='upper left')
		outputGraph = "algo_" + str(time.strftime("%Y-%m-%d_%H-%M-%S"))
		plt.savefig("output/" + outputGraph, bbox_inches='tight')
		time.sleep(1)
	plt.show()





#==============================================================================================

def runMaster():
	"""	Loads backtest data, and runs the backtest."""
	global TRAINING_STOCK, BACKTEST_STOCK, SELECT_STOCKS
	algo_obj = TradingAlgorithm(initialize=initialize, handle_data=handle_data)	
	perf_manual = []
	for stock in SELECT_STOCKS:
		BACKTEST_STOCK = stock
		backtestData = Manager.loadData(2013, 2013+BACKTEST_TIME, stock_list=[stock, 'SPY']) #, startM=1, endM=2, 
		print "Create algorithm..."
		perf_manual.append(algo_obj.run(backtestData))
	analyze(perf_manual)

#==============================================================================================

def graphClusters(clusters):
	"""
		Parameters:
			clusters (list): list of list(cluster) of list(stock) of doubles
			dateList (list): list of dates 
	"""
	plt.figure("Clusters")
	for i, cluster in enumerate(clusters):
		for stock in clusters[i]:
			plt.subplot(2,5,i+1)
			plt.ylabel("Cluster" + str(i))
			plt.plot(stock)
	plt.show()

#==============================================================================================

def runClusters():
	global TRAINING_STOCK, BACKTEST_STOCK, SELECT_STOCKS
	
	stock_data_list = Manager.getStockDataList(TRAINING_TIME)

	kmeans = KMeans(n_clusters=10)
	kmeans_data = [np.array(x).ravel() for x in stock_data_list]
	kmeans.fit(kmeans_data)
	clusters = {}
	for i, label in enumerate(kmeans.labels_):
		if label in clusters:
			clusters[label].append(stock_data_list[i])
		else:
			clusters[label] = list()
			clusters[label].append(stock_data_list[i])	

	print "# of Clusters: " + str(len(clusters))
	print "# of stocks: " + str(len(stock_data_list))
	print "Cluster sizes: "
	for key in clusters:
		print str(key) + ": " + str(len(clusters[key]))
	
	#print "Calculating for elbow method..."
	#inertia = []
	#for x in range(1, 100):
	#	kmeans = KMeans(n_clusters=x)
	#	kmeans.fit([np.array(x).ravel() for x in stock_data_list])
	#	inertia.append(kmeans.inertia_)
	#plt.subplot(1,2,2)
	#plt.plot(inertia)
	
	graphClusters(clusters)

	#strategy = STRATEGY_CLASS([normalized_data], [target], num_epochs=EPOCHS)



#==============================================================================================

def main():
	"""	Allows for switching easily between different tests using a different STRATEGY_CLASS.
		strategy_num: Must pick a STRATEGY_CLASS from the strategy_dict.
		training_time: years since 2002
		backtest_time: years since 2002
	"""	
	global STRATEGY_CLASS, TRAINING_TIME, BACKTEST_TIME, EPOCHS, IS_NORMALIZE
	
	parser = argparse.ArgumentParser()
	parser.add_argument("-n", "--strategy_num", default=1, type=int, choices=[key for key in strategy_dict])
	parser.add_argument("-t", "--training_time", default=1, type=int, choices=[year for year in range(0,14)])
	parser.add_argument("-b", "--backtest_time", default=1, type=int, choices=[year for year in range(0,14)])
	parser.add_argument("-e", "--epochs", default=2000, type=int)
	parser.add_argument("-z", "--normalize", action='store_false', help='Turn normalization off.')
	parser.add_argument("-o", "--overfit", action='store_false', help='Perform test with overfitting.')
	args = parser.parse_args()
	
	STRATEGY_CLASS = strategy_dict[args.strategy_num]
	TRAINING_TIME = args.training_time
	BACKTEST_TIME = args.backtest_time
	EPOCHS = args.epochs
	IS_NORMALIZE = args.normalize
	IS_OVERFIT = args.overfit
	if not IS_OVERFIT : #or not IS_NORMALIZE
		print "ERROR: not implemented"; return
	print "Using:", str(STRATEGY_CLASS)
	print "Train", TRAINING_TIME, "year,", "Test", BACKTEST_TIME, "year."
	#runMaster()
	runClusters()

#==============================================================================================

if __name__ == "__main__":
	main()

#==============================================================================================


