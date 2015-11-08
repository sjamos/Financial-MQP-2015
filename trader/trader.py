"""
    trader.py

    Contains initialize() and handle_data() for TradingAlgorithm

"""

from zipline.api import order, record, symbol, history, add_history, get_open_orders, order_target_percent

#==============================================================================================

def initialize(context):
    print "Initialize..."
    global STRATEGY_OBJECT, PRE_TRAINING_DATA, BACKTEST_STOCK
    context.security = symbol(BACKTEST_STOCK)
    context.benchmark = symbol('SPY')
    context.strategy = STRATEGY_OBJECT
    context.raw_data = TRAINING_DATA
    context.normalized_data = Manager.normalize(context.raw_data)
    print "Backtest symbol:", context.security
    print "Capital Base:", context.portfolio.cash

#==============================================================================================

# Gets called every time-step
def handle_data(context, data):
    #assert context.portfolio.cash > 0.0, "ERROR: negative context.portfolio.cash"
    assert len(context.training_data) == context.training_data_length; "ERROR: "
    
    # data stored as (open, high, low, close, volume, price)
    feed_data = ([  
                    data[context.security].open, 
                    data[context.security].high,
                    data[context.security].low,
                    data[context.security].close
                    #data[context.security].volume,
                    #data[context.security].close,
    ])
    
    #keep track of history. 
    context.raw_data.pop(0)
    context.raw_data.append(feed_data)
    context.normalized_data = Manager.normalize(context.raw_data, IS_NORMALIZE)
    prediction = context.strategy.predict(context.normalized_data)[-1]
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
