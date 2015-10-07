# Financial MQP 2015

	

### Team Members
* Essam Al-Mansouri
* Sean Amos
* Nicholas Bradford

### Usage
#### Cluster Demo
  In directory ./cluster/:
  
	$ python runner.py

### Dependencies

* Theano (requires gcc): http://deeplearning.net/software/theano/install.html
* Anaconda: http://docs.continuum.io/anaconda/install
* Quantopian Zipline: https://github.com/quantopian/zipline
	
### Goals
Sentiment Analysis
* Due to limited data, find correlations only over the past couple years
* Predict large swings in specific stocks
* Aggregate findings into sentiment of entire market
* Produce % likelihood of large increase/decrease in value

Portfolio Manager
* Cluster S&P 100/500 by sector, then Market Cap, Volume, Volatility, P/E, TimeSeries
* Train RNNs on each cluster, evaluating based on average performance
* Re-cluster based on calculated divergence
* Select best performing companies/clusters to add to portfolio

Neural Network Trader (LSTM Recurrent Neural Net)
* Train on hourly bars, produce Buy/Sell signal (with variable strength) as output
* Generalizing to arbitrary bar size, if possible
* Compare final results to S&P 500 (10 years training, 1 year testing)
