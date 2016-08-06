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
import sys

import pprint
import multiprocessing
import time
#########################################################################################################################
# CONFIGURATIONS
#########################################################################################################################

# Setup for Logging, Global Variable Definitions, 
# logging.basicConfig(filename='etl/LocationCounter.log',level=logging.INFO,format='%(asctime)s %(levelname)s: %(message)s')
# config = configparser.ConfigParser()
# config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),"../","crawlers/","newscrawler.conf"))
run_path = os.path.abspath(os.path.dirname(__file__))
logging.basicConfig(filename='yahooFinance.log',level=logging.INFO,format='%(asctime)s %(levelname)s: %(message)s')

output_path = run_path +"/stockdata/"
output_file = output_path + 'stockdata.csv'
nasdaq_file = run_path + "/nasdaq_list.csv"

# Setup CSV File for Writing Stocks
# Functions for output file formatting
def writeStockdataHeader(output_file):
	if not os.path.exists(output_path):
		os.makedirs(output_path)

	if os.path.isfile(output_file):
		os.remove(output_file)
	''' Write Header to the Stock Price Output File '''
	with open(output_file, 'w') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow(["Symbol","Date","High","Low","Close","AdjClose","Open","Volume"])
	
	return

def writeStockdataRow(output_file, symbol,date,high,low,close,adjclose,opn,volume):
	''' Logic for writing a stock date row, needs to be moved to a single threaded function '''
	
	# csv_start = time.time()
	logging.debug("Metadata path is {}".format(output_file))

	#Write metadata and reference to full text file name to csv separated by ~ character
	with open(output_file, 'a') as csvfile:
		Writer = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		Writer.writerow([symbol,date,high,low,close,adjclose,opn,volume])
	# logging.debug("CSV write completed to {} in {} seconds".format(output_file,time.time()- csv_start))
	
	return

def readNasdaqList():
	''' Pulls names of NASDAQ tickers into an array from CSV file '''

	tickers = []
	with open(nasdaq_file, 'r') as csvfile:
		csvReader = csv.reader(csvfile, delimiter=',')
		next(csvReader)
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



def processStockData(ticker):
	
	stock_data = fetchStockData(ticker)
	parseStockData(stock_data)

def fetchStockData(ticker):
	stock_data = {}

	try:
		print("Fetching for:" + ticker)
		stock_data[ticker] = stocks.get_historical_info(ticker)
		# pprint.pprint(stock_data[ticker])
		
	except KeyError:
		print("Key not found:" + ticker) 
	except stocks.NoResultsError:
		print("No Results found for:" + ticker)

	return stock_data

def parseStockData(stock_data):
	for ticker in stock_data:
		for date in stock_data[ticker]:
			print("Writing for" + ticker, date['Date'])
			writeStockdataRow(output_file, ticker,
				date["Date"], 
				date["High"], 
				date["Low"], 
				date["Close"], 
				date["AdjClose"], 
				date["Open"], 
				date["Volume"])

def main():
	tickers = readNasdaqList()
	
	writeStockdataHeader(output_file)
	
	pool = multiprocessing.Pool()
	pool.map(processStockData,tickers)

if __name__ == '__main__':
	main()