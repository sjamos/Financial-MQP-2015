# Financial MQP 2015

Demo version released 11/10/2015.

### Team Members
* Essam Al-Mansouri
* Sean Amos
* Nicholas Bradford

### TODO

* Add command-line option for trading/backtest start_date/end_date
* Add large-scale comparison test for clustering vs. not clustering
* Add volume
* Add IS_NORMALIZE
* Add IS_OVERFIT

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

    ./manager/example_backtest $ run_algo.py -f movingAverages.py --start 2000-1-1 --end 2014-1-1 --symbols AAPL -o movingAverages_out.pickle
    ./manager/example_backtest $ python readfile.py

### Dependencies

* NumPy, SciPy, scikit-learn (use Anaconda): http://docs.continuum.io/anaconda/install
* Lasagne (requires Theano): http://lasagne.readthedocs.org/en/latest/user/installation.html
* Quantopian Zipline (backtesting): https://github.com/quantopian/zipline
