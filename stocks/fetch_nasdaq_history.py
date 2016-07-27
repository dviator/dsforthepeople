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

OUTFILE_STUB = "/home/alex/Desktop/dsforthepeople/stocks/stockdata/"
NASDAQ = "/home/alex/Desktop/dsforthepeople/stocks/nasdaq_list.csv"

# Setup CSV File for Writing Stocks
# Functions for output file formatting
def writeStockdataHeader(filename):
	''' Write Header to the Stock Price Output File '''
	with open(filename, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow(["Symbol","Date","High","Low","Close","AdjClose","Open","Volume"])
	
	return

def writeStockdataRow(OUTFILE, symbol,date,high,low,close,adjclose,opn,volume):
	''' Logic for writing a stock date row, needs to be moved to a single threaded function '''
	
	# csv_start = time.time()
	# logging.debug("Metadata path is {}".format(OUTFILE))

	#Write metadata and reference to full text file name to csv separated by ~ character
	with open(OUTFILE, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow([symbol,date,high,low,close,adjclose,opn,volume])
	# logging.debug("CSV write completed to {} in {} seconds".format(OUTFILE,time.time()- csv_start))
	
	return

def readNasdaqList():
	''' Pulls names of NASDAQ tickers into an array from CSV file '''

	tickers = []
	with open(NASDAQ, 'r') as csvfile:
		csvReader = csv.reader(csvfile, delimiter=',')
		csvReader.next()
		for row in csvReader:
			tickers.append(row[0])

	return tickers

# Get current stock information - returns most of 
# the information on a typical Yahoo! Finance stock page
# info = stocks.get_current_info(["YHOO","AAPL","GOOG","MSFT"])
# print info

# Get current stock news - returns the RSS feed for
# the given ticker in JSON format
# news = stocks.get_news_feed('YHOO')

# Get historical prices - returns all historical
# open/low/high/close/volumn for thie given ticker

tickers = readNasdaqList()

for item in tickers:
	# Delete the output file if it currently exists
	filename = OUTFILE_STUB + item + ".csv"
	if os.path.isfile(filename):
		os.remove(filename)
	# Create it again
	writeStockdataHeader(filename)

datum = {}
for ticker in tickers:
	print ticker
	# FIXME: some symbols will not register with YAHOO for some reason, need to pass anyways
	try:
		datum[ticker] = stocks.get_historical_info(ticker)
	except:
		pass

for ticker in datum:
	for date in datum[ticker]:
		writeStockdataRow(OUTFILE_STUB + ticker + ".csv", ticker,
			date["Date"], 
			date["High"], 
			date["Low"], 
			date["Close"], 
			date["AdjClose"], 
			date["Open"], 
			date["Volume"])
