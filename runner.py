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
import argparse

import numpy as np
np.random.seed(13)

# clustering, managing, and neural nets
from manager.manager import Manager
from neuralnets.tradingnet import TradingNet

# handling dates
import pytz
from datetime import datetime

# backtesting
from zipline.api import order, record, symbol, history, add_history, get_open_orders, order_target_percent
from zipline.algorithm import TradingAlgorithm
from zipline.utils.factory import load_bars_from_yahoo

# analysis
import matplotlib.pyplot as plt


# glabal strategy assigned in main()
global STRATEGY_CLASS 
strategy_dict = {
	1: TradingNet
}

def testNN():
	x = [
		[[0.1],[0.2],[0.3],[0.4],[0.5]],
		[[-0.1],[-0.2],[-0.3],[-0.4],[-0.5]]
	]
	y = [
		[[1],[0],[1],[0],[1]],
		[[1],[0],[1],[0],[1]]
	]
	#In this example...
	#Number of sequences: 2
	#Number of timesteps per sequence: 5
	#Number of inputs per timestep: 1
	testnet = TradingNet(x, y, num_epochs=2)
	testprediction = testnet.predict([[0.1],[0.2],[0.3],[0.4],[0.5]]) #input is one full sequence
	print(testprediction) #expected: [[1],[0],[1],[0],[1]]



def loadTrainingData():
	print "Load training data..."
	start = datetime(2010, 1, 1, 0, 0, 0, 0, pytz.utc)
	end = datetime(2010, 3, 1, 0, 0, 0, 0, pytz.utc)
	data = load_bars_from_yahoo(stocks=['SPY'], 
								start=start,
								end=end)
	# data stored as (open, high, low, close, volume, price)
	answer = data.transpose(2, 1, 0, copy=True).to_frame()	# pandas.Panel --> pandas.DataFrame
	answer = answer.values.tolist() 						# pandas.DataFrame --> List of Lists
	# only take adjusted (open, high, low, close)
	answer = ([ 							
				([	x[0] - x[0], 			# open
					x[1] - x[0],	# high 	- open
					x[2] - x[0],	# low 	- open
					x[3] - x[0],	# close - open
					#data[context.security].volume,
					#data[context.security].close,
				]) for x in answer
	])
	return answer


# Define algorithm
def initialize(context):
	print "Initialize..."
	context.security = symbol('SPY')
	context.training_data = loadTrainingData()
	context.training_data_length = len(context.training_data) - 1
	context.normalized_data = Manager.normalize(context.training_data) 	# will have to redo every time step
	
	target = Manager.getTargets(context.normalized_data)
	context.training_data = context.training_data[:-1]			# delete last data entry, because it won't be used
	context.normalized_data = context.normalized_data[:-1] 		# delete last data entry, because it won't be used
	#print target
	plt.figure(1)
	for i in range(len(context.normalized_data[0])):
		plt.plot([x[i] for x in context.normalized_data])
	plt.legend(['open', 'high', 'low', 'close', 'volume', 'price'], loc='upper left')
	plt.show()

	print "Train..."
	#print len(context.training_data), len(context.normalized_data), len(target)
	context.strategy = STRATEGY_CLASS([context.normalized_data], [target], num_epochs=2000)
	
	print "Capital Base: " + str(context.portfolio.cash)






# Gets called every time-step
def handle_data(context, data):
	#print "Cash: $" + str(context.portfolio.cash), "Data: ", str(len(context.training_data))
	#assert context.portfolio.cash > 0.0, "ERROR: negative context.portfolio.cash"
	assert len(context.training_data) == context.training_data_length; "ERROR: "

	# data stored as (open, high, low, close, volume, price)
	feed_data = ([	
					data[context.security].open 	- data[context.security].open, 
					data[context.security].high 	- data[context.security].open,
					data[context.security].low 		- data[context.security].open,
					data[context.security].close 	- data[context.security].open
					#data[context.security].volume,
					#data[context.security].close,
	])
	#keep track of history. 
	context.training_data.pop(0)
	context.training_data.append(feed_data)
	context.normalized_data = Manager.normalize(context.training_data) # will have to redo every time step
	#print len(context.training_data), len(context.normalized_data), len(context.normalized_data[0])

	prediction = context.strategy.predict(context.training_data)[-1]
	print "Value:", context.portfolio.portfolio_value, "Cash:", str(context.portfolio.cash), "\tPredict:", str(prediction[0])

	# Do nothing if there are open orders:
	if has_orders(context, data):
		print('has open orders - doing nothing!')
	# Put entire position in
	elif prediction > 0.5:
		order_target_percent(context.security, .99)
		print "BUY!"
	# Take entire position out
	else:
		order_target_percent(context.security, 0)
		#order_target_percent(context.security, -.99)
		print "SELL!"

	record(SPY=data[context.security].price)


def has_orders(context, data):
	# Return true if there are pending orders.
	has_orders = False
	for stock in data:
		orders = get_open_orders(stock)
		if orders:
			for oo in orders:
				message = 'Open order for {amount} shares in {stock}'  
				message = message.format(amount=oo.amount, stock=stock)
				log.info(message)
				has_orders = True
			return has_orders


# to be called after the backtest
def analyze(perf):
	print "Analyze..."
	plt.figure(1)

	"""
	# manager.normalizeByZScore()
	plt.subplot(211)
	plt.plot(perf.portfolio_value)
	plt.plot(perf.SPY)
	"""

	ax1 = plt.subplot(211)
	perf.portfolio_value.plot(ax=ax1)
	ax1.set_ylabel('portfolio value')
	
	ax2 = plt.subplot(212, sharex=ax1)
	perf.SPY.plot(ax=ax2)
	ax2.set_ylabel('SPY stock price')

	try:
		plt.figure(2)
		plt.plot([x/perf.portfolio_value[0] for x in perf.portfolio_value])
		plt.plot([x/perf.SPY[0] for x in perf.SPY])
		plt.legend(['algorithm', 'SPY'], loc='upper left')
	except:
		print "Graphing error in analyze()!"
	
	plt.show()




def runMaster():
	"""	Training data for the NN will be for 2002-2012
		Testing will be 2012-2015 (will pick up right where it left off)
	"""

	# load data, stored as (open, high, low, close, volume, price)
	start = datetime(2010, 1, 1, 0, 0, 0, 0, pytz.utc)
	end = datetime(2010, 6, 1, 0, 0, 0, 0, pytz.utc)
	data = load_bars_from_yahoo(stocks=['SPY'], 
								start=start,
								end=end)
	
	DATA = data.transpose(2, 1, 0, copy=True).to_frame()	# pandas.Panel --> pandas.DataFrame
	HISTORY = DATA.values.tolist() 							# pandas.DataFrame --> List of Lists

	print "Create algorithm..."
	algo_obj = TradingAlgorithm(initialize=initialize, 
								handle_data=handle_data)
	perf_manual = algo_obj.run(data)
	analyze(perf_manual)


def main():
	"""	Allows for switching easily between different tests using a different STRATEGY_CLASS."""
	
	# must pick a STRATEGY_CLASS from the strategy_dict
	global STRATEGY_CLASS

	# Argument parser
	parser = argparse.ArgumentParser()
	parser.add_argument("strategy_num", type=int, choices=[key for key in strategy_dict])
	args = parser.parse_args()
	STRATEGY_CLASS = strategy_dict[args.strategy_num]
	print "Using:", str(STRATEGY_CLASS)
	#testNN()
	runMaster()


if __name__ == "__main__":
	main()
