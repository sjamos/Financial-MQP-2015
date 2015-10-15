"""
	runner.py

	When creating a new class, you must ensure that it:
		-accepts a list of lists of stock data in its constructor (data, target, num_epochs)
		-has a predict() method
		-is added to the global strategy_dict{}

	Usage:
		$ python runner.py [strategy_num]

"""

import sys
import time
import argparse

# seed to prevent
import numpy as np
np.random.seed(13)

# clustering, managing, and neural nets
from manager.manager import Manager
from neuralnets.tradingnet import TradingNet
from neuralnets.deepnet import DeepNet, testNN

# handling dates
import pytz
from datetime import datetime

# backtesting
from zipline.api import order, record, symbol, history, add_history, get_open_orders, order_target_percent
from zipline.algorithm import TradingAlgorithm
from zipline.utils.factory import load_bars_from_yahoo

# analysis
import matplotlib.pyplot as plt

# glabal strategy assigned in main(), along with the years to train, years to backtest, and epochs to train
global STRATEGY_CLASS, TRAINING_TIME, BACKTEST_TIME, EPOCHS, IS_NORMALIZE
global TRAINING_STOCK, BACKTEST_STOCK # Training and backtest stock assigned in runMaster()
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
	
	context.training_data = loadTrainingData(TRAINING_TIME, TRAINING_STOCK)
	context.training_data_length = len(context.training_data) - 2
	context.normalized_data = Manager.normalize(context.training_data) 	# will have to redo every time step
	
	target = Manager.getTargets(context.normalized_data)
	context.training_data = context.training_data[:-2]			# delete last data entry, because it won't be used
	context.normalized_data = context.normalized_data[:-2] 		# delete last data entry, because it won't be used
	#print target
	#plt.figure("Training Data")
	#for i in range(len(context.normalized_data[0])):
	#	plt.plot([x[i] for x in context.normalized_data])
	#plt.legend(['open', 'high', 'low', 'close', 'volume', 'price'], loc='upper left')
	#plt.show()

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
	#plt.show()

#==============================================================================================

def loadTrainingData(training_time, training_stock):
	"""	Data stored as (open, high, low, close, volume, price)
		Only take adjusted (open, high, low, close)
	"""
	print "Load training data..."
	answer = loadData(2002, 2002+training_time, stock_list=[training_stock])
	answer = loadConvertDataFormat(answer)
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

def loadConvertDataFormat(data):
	answer = data.transpose(2, 1, 0, copy=True).to_frame()	# pandas.Panel --> pandas.DataFrame
	answer = answer.values.tolist() 						# pandas.DataFrame --> List of Lists
	return answer

#==============================================================================================

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

def runMaster():
	"""	Loads backtest data, and runs the backtest."""
	global TRAINING_STOCK, BACKTEST_STOCK 
	TRAINING_STOCK = 'SPY'
	SELECT_STOCKS = ['AAPL', 'DIS', 'XOM', 'UNH', 'WMT']
	algo_obj = TradingAlgorithm(initialize=initialize, handle_data=handle_data)	
	perf_manual = []
	for stock in SELECT_STOCKS:
		BACKTEST_STOCK = stock
		backtestData = loadData(2002, 2002+BACKTEST_TIME, stock_list=[stock, 'SPY']) #, startM=1, endM=2, 
		print "Create algorithm..."
		perf_manual.append(algo_obj.run(backtestData))
	analyze(perf_manual)


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
	if not IS_OVERFIT or not IS_NORMALIZE:
		print "ERROR: not implemented"; return
	print "Using:", str(STRATEGY_CLASS)
	print "Train", TRAINING_TIME, "year,", "Test", BACKTEST_TIME, "year."
	runMaster()

#==============================================================================================

if __name__ == "__main__":
	main()

#==============================================================================================



stocks_DJIA = ([
				'MMM', 
				'AXP', 
				'AAPL',
				'BA', 
				'CAT', 
				'CVX', 
				'CSCS', 
				'KO', 
				'DIS', 
				'DD', 
				'XOM',
				'GE',
				'G',
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
				'V',
				'WMT'
])