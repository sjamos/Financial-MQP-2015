# Financial MQP 2015

	

### Team Members
* Essam Al-Mansouri
* Sean Amos
* Nicholas Bradford

### Usage

Running master tests: 

    runner.py   [-h] 
                [-n {1,2,3}] 
                [-t {0,1,2,3,4,5,6,7,8,9,10,11,12,13}]
                [-b {0,1,2,3,4,5,6,7,8,9,10,11,12,13}] 
                [-e EPOCHS]
                [-z]

    optional arguments:
      -h, --help            show this help message and exit
      -n {1,2,3}, --strategy_num {1,2,3}
      -t {0,1,2,3,4,5,6,7,8,9,10,11,12,13}, --training_time {0,1,2,3,4,5,6,7,8,9,10,11,12,13}
      -b {0,1,2,3,4,5,6,7,8,9,10,11,12,13}, --backtest_time {0,1,2,3,4,5,6,7,8,9,10,11,12,13}
      -e EPOCHS, --epochs EPOCHS
      -z, --normalize       Turn normalization off.

Verify Quantopian Zipline framework:

    ./backtest $ run_algo.py -f movingAverages.py --start 2000-1-1 --end 2014-1-1 --symbols AAPL -o movingAverages_out.pickle
    ./backtest $ python readfile.py

### Dependencies

* NumPy, SciPy, scikit-learn (use Anaconda): http://docs.continuum.io/anaconda/install
* Lasagne (requires Theano): http://lasagne.readthedocs.org/en/latest/user/installation.html
* Quantopian Zipline (backtesting): https://github.com/quantopian/zipline
	
### Goals
Sentiment Analysis:
* Due to limited data, find correlations only over the past couple years
* Predict large swings in specific stocks
* Aggregate findings into sentiment of entire market
* Produce % likelihood of large increase/decrease in value

Portfolio Manager:
* Cluster S&P 100/500 by sector, then Market Cap, Volume, Volatility, P/E, TimeSeries
* Train RNNs on each cluster, evaluating based on average performance
* Re-cluster based on calculated divergence
* Select best performing companies/clusters to add to portfolio

Neural Network Trader (LSTM Recurrent Neural Net):
* Train on hourly bars, produce Buy/Sell signal (with variable strength) as output
* Generalizing to arbitrary bar size, if possible
* Compare final results to S&P 500 (10 years training, 1 year testing)
