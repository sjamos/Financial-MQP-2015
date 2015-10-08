"""
	runner.py

	Usage:
		$ python runner.py

"""

# clustering, managing, and neural nets
from cluster.manager import Manager
from neuralnets.tradingnet import TradingNet

# handling dates
import pytz
from datetime import datetime

# backtesting
from zipline.api import order, record, symbol
from zipline.algorithm import TradingAlgorithm
from zipline.utils.factory import load_bars_from_yahoo

# analysis
import matplotlib.pyplot as plt


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

# Define algorithm
def initialize(context):
   	print "Initialize..."
   	context.manager = Manager()
   	context.security = symbol('SPY')

   	print "Load NN training data..."
   	data, target = context.manager.getXandY("Data/SPY_1993_to_2015.csv")
   	#print target

   	print "Train NN..."
   	context.neuralnet = TradingNet([data], [target], num_epochs=2)
   	print "Starting cash: " + str(context.portfolio.cash)

def handle_data(context, data):
    myCash = context.portfolio.cash
    quantity = myCash / data[context.security].price

    # openList, highList, lowList, closeList
    feedData = (
    			[[	data[context.security].open, 
	    			data[context.security].high 	- data[context.security].open,
	    			data[context.security].low 		- data[context.security].open,
	    			data[context.security].close 	- data[context.security].open]]
    )
    #print feedData
    prediction = context.neuralnet.predict(feedData)[0]

    if prediction > 0.5:
    	order(context.security, 10)
    else:
    	order(context.security, -context.portfolio.positions[context.security].amount)

    #if myCash > data[symbol('SPY')].price * 10:
    #	order(symbol('SPY'), 10)
    record(SPY=data[context.security].price)
    #print ".",
    print context.portfolio.cash

def before_trading_start(context, data):
	pass

def analyze(perf, manager):
	fig = plt.figure()

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
	
	plt.show()


def runMaster():
	manager = Manager()

	# Load data manually from Yahoo! finance
	print "Load data..."
	start = datetime(2011, 1, 1, 0, 0, 0, 0, pytz.utc)
	end = datetime(2012, 1, 1, 0, 0, 0, 0, pytz.utc)
	data = load_bars_from_yahoo(stocks=['SPY'], 
								start=start,
	                            end=end)

	# Create algorithm object passing in necessary functions
	print "Create algorithm..."
	algo_obj = TradingAlgorithm(initialize=initialize, 
	                            handle_data=handle_data,
	                            before_trading_start=before_trading_start)

	# Run algorithm
	print "Run..."
	perf_manual = algo_obj.run(data)
	#print perf_manual.head()
	
	print "Analyze..."
	analyze(perf_manual, manager)

# switch easily between different testsO
def main():
	#testNN()
	runMaster()

if __name__ == "__main__":
	main()
