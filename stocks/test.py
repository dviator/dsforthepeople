#########################################################################################################################
# IMPORT STATEMENTS
#########################################################################################################################

# for reading in configurations and paths
import logging
## FIXME: stockretriever needs to be rewritten to be compatible with python3
# import configparser
import os
import ntpath

# library for retrieving stocks via yql from yahoo finance
import stockretriever as stocks

# for writing stock data
import csv

#########################################################################################################################
# CONFIGURATIONS
#########################################################################################################################

# Setup for Logging, Global Variable Definitions, 
# logging.basicConfig(filename='etl/LocationCounter.log',level=logging.INFO,format='%(asctime)s %(levelname)s: %(message)s')
# config = configparser.ConfigParser()
# config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),"../","crawlers/","newscrawler.conf"))

OUTFILE = "/home/alex/Desktop/dsforthepeople/stocks/stockdata_yahoo.csv"

# Setup CSV File for Writing Stocks
# Functions for output file formatting
def writeStockdataHeader(filename):
	''' Write Header to the Stock Price Output File '''
	with open(filename, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow(["Symbol","Date","High","Low","Close","AdjClose","Open","Volume"])
	
	return

def writeStockdataRow(symbol,date,high,low,close,adjclose,opn,volume):
	''' Logic for writing a stock date row, needs to be moved to a single threaded function '''
	
	# csv_start = time.time()
	# logging.debug("Metadata path is {}".format(OUTFILE))

	#Write metadata and reference to full text file name to csv separated by ~ character
	with open(OUTFILE, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow([symbol,date,high,low,close,adjclose,opn,volume])
	# logging.debug("CSV write completed to {} in {} seconds".format(OUTFILE,time.time()- csv_start))
	
	return

# Get current stock information - returns most of 
# the information on a typical Yahoo! Finance stock page
# info = stocks.get_current_info(["YHOO","AAPL","GOOG","MSFT"])
# print info

# Get current stock news - returns the RSS feed for
# the given ticker in JSON format
# news = stocks.get_news_feed('YHOO')

# Get historical prices - returns all historical
# open/low/high/close/volumn for thie given ticker

writeStockdataHeader(OUTFILE)
news = stocks.get_historical_info('YHOO')
for entry in news:
	writeStockdataRow("YHOO", entry["Date"], entry["High"], entry["Low"], entry["Close"], entry["AdjClose"], entry["Open"], entry["Volume"])
