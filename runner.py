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
from zipline.api import order, record, symbol, history, add_history, get_open_orders, order_target_percent
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



def loadTrainingData():
	print "Load training data..."
	start = datetime(2002, 1, 1, 0, 0, 0, 0, pytz.utc)
	end = datetime(2011, 1, 1, 0, 0, 0, 0, pytz.utc)
	data = load_bars_from_yahoo(stocks=['SPY'], 
								start=start,
	                            end=end)
	# data stored as (open, high, low, close, volume, price)
	answer = data.transpose(2, 1, 0, copy=True).to_frame()	# pandas.Panel --> pandas.DataFrame
	answer = answer.values.tolist() 						# pandas.DataFrame --> List of Lists
	return answer



# Define algorithm
def initialize(context):
   	print "Initialize..."
   	#context.manager = Manager()
   	context.security = symbol('SPY')
   	context.training_data = loadTrainingData()
   	context.training_data_length = len(context.training_data) - 1

   	print "Load NN targets..."
   	target = Manager.getTargets(context.training_data)

   	print "Train NN..."
   	context.neuralnet = TradingNet([context.training_data], [target], num_epochs=2)
   	
   	print "Capital Base: " + str(context.portfolio.cash)



# Gets called every time-step
def handle_data(context, data):
    #print "Cash: $" + str(context.portfolio.cash), "Data: ", str(len(context.training_data))
    assert context.portfolio.cash > 0.0, "ERROR: negative context.portfolio.cash"
    assert len(context.training_data) == context.training_data_length

    myCash = context.portfolio.cash < 0
    quantity = myCash / data[context.security].price

    # openList, highList, lowList, closeList
    # data stored as (open, high, low, close, volume, price)
    feed_data = (
    			[	
    				data[context.security].open, 
	    			data[context.security].high, 		# - data[context.security].open
	    			data[context.security].low, 		# - data[context.security].open
	    			data[context.security].close, 		# - data[context.security].open
	    			data[context.security].volume,
	    			data[context.security].close,
				]
	)
    #keep track of history. 
    context.training_data.pop(0)
    context.training_data.append(feed_data)
    prediction = context.neuralnet.predict(context.training_data)[-1]
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
    	print "SELL!"

    record(SPY=data[context.security].price)
    print context.portfolio.cash #print ".",


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



# Training data for the NN will be for 2002-2012
# Testing will be 2012-2015 (will pick up right where it left off)
def runMaster():
	#manager = Manager()
	#print "Load NN training data..."
	#start = datetime(2002, 1, 1, 0, 0, 0, 0, pytz.utc)
	#end = datetime(2012, 1, 1, 0, 0, 0, 0, pytz.utc)

	print "Load data..."
	start = datetime(2012, 1, 2, 0, 0, 0, 0, pytz.utc)
	end = datetime(2012, 2, 2, 0, 0, 0, 0, pytz.utc)
	data = load_bars_from_yahoo(stocks=['SPY'], 
								start=start,
	                            end=end)
	# stored as (open, high, low, close, volume, price)
	DATA = data.transpose(2, 1, 0, copy=True).to_frame()	# pandas.Panel --> pandas.DataFrame
	HISTORY = DATA.values.tolist() 							# pandas.DataFrame --> List of Lists
	#print DATA
	#print HISTORY[0] 

	print "Create algorithm..."
	algo_obj = TradingAlgorithm(initialize=initialize, 
	                            handle_data=handle_data)
	
	print "Run..."
	perf_manual = algo_obj.run(data)
	#print perf_manual.head()
	
	print "Analyze..."
	analyze(perf_manual)



# switch easily between different testsO
def main():
	#testNN()
	runMaster()

if __name__ == "__main__":
	main()
