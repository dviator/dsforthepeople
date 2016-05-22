import mechanicalsoup
import json
import datetime
import time
import logging
import configparser
import sys
import requests
import os
from retrying import retry

# config = configparser.ConfigParser()
# config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),"newscrawler.conf"))
# start_date_year = config.getint('nytimes','start_date_year')
# start_date_month = config.getint('nytimes','start_date_month')
# start_date_day = config.getint('nytimes','start_date_day')
# end_date_year = config.getint('nytimes','end_date_year')
# end_date_month = config.getint('nytimes','end_date_month')
# end_date_day = config.getint('nytimes','end_date_day')

def getConfig():
	config = configparser.ConfigParser()
	config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),"newscrawler.conf"))
	start_date_year = config.getint('nytimes','start_date_year')
	start_date_month = config.getint('nytimes','start_date_month')
	start_date_day = config.getint('nytimes','start_date_day')
	end_date_year = config.getint('nytimes','end_date_year')
	end_date_month = config.getint('nytimes','end_date_month')
	end_date_day = config.getint('nytimes','end_date_day')
	return start_date_year, start_date_month, start_date_day, end_date_year, end_date_month, end_date_day

def crawl_nytimes_archive(queue):
	
	browser = mechanicalsoup.Browser()

	#Fetch date targets from config file
	start_date_year, start_date_month, start_date_day, end_date_year, end_date_month, end_date_day = getConfig()
	#Define Date Values so they can be incremented in search URL
	end_date = datetime.date(end_date_year,end_date_month,end_date_day)
	#Defines the date to start crawling at. 
	target_date = datetime.date(start_date_year,start_date_month,start_date_day)
	
	#Loop through search results by calling search URL for each date in range one at a time
	while target_date < end_date:
		#Set search_url based on target_date
		search_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date={0:d}{1:02d}{2:02d}&begin_date={3:d}{4:02d}{5:02d}&page=1&facet=true".format(target_date.year,target_date.month,target_date.day,target_date.year,target_date.month,target_date.day)
		print(search_url)
		date_start_time = time.time()
		
		page_number = 1
		#Loop through each page of search results
		while page_number < 100:
			#Queue all the links for this search page
			crawlPage(target_date, browser, search_url, queue, page_number)
			#Get new page number and preserve old to perform string replacement in new url.
			old_page_number = page_number
			page_number += 1
			#Increment searc url page number by one by reversing url and switching old_page_number with new
			search_url = search_url[::-1].replace(str(old_page_number)[::-1],str(page_number)[::-1],1)[::-1]

		logging.info("Day {} queued in {} seconds".format(target_date,(time.time() - date_start_time)))
		#Increment date to search new url when all pages for last date are complete. 
		target_date += datetime.timedelta(days=1)

def crawlPage(target_date, browser, search_url, queue, page_number):
	page_start_time = time.time()
	logging.debug("Base nytimes Search URL = {}".format(search_url))
	#Raise exception if network IO fails but continue execution
	try:	
		search_page = getSearchJSON(browser, search_url)
	except requests.exceptions.MissingSchema:
		logging.exception("nytimes queueing Error on page: {}".format(search_url))
		return
	#Parse article URLS from searchJSON
	try:
		article_urls = parseSearchJSON(search_page)
	except KeyError:
		logging.exception('Search page at url: {} had malformed response'.format(search_url))
		return
	#If no exceptions, place article links on queue
	else:
		queueArticles(article_urls,queue,target_date)
		print("Day {} Page number {} queued in {} seconds".format(target_date.strftime('%m-%d-%Y'),page_number,time.time() - page_start_time))

	return 


def retry_if_request_error(exception):
	if isinstance(exception,requests.exceptions.ConnectionError):
		return True
	elif isinstance(exception,requests.exceptions.ReadTimeout):
		return True
	else:
		return False

@retry(retry_on_exception=retry_if_request_error,wait_exponential_multiplier=1000,wait_exponential_max=30000,stop_max_delay=600000)
def getSearchJSON(browser,search_url):
	search_page = browser.get(search_url, timeout=5)
	logging.debug("Got a search page going to return")
	return search_page
 
def parseSearchJSON(search_page):
	print(type(search_page))
	response = search_page.json()['response']
	print(type(response))
	article_urls = []
	for snippet in response['docs']:
		#Place article link on queue
		article_url = snippet['web_url']
		article_urls.append(article_url)
	return article_urls


def queueArticles(article_urls,queue,target_date):
	for article_url in article_urls:
		queue.put((article_url,"nytimes",target_date))
