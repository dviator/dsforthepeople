import mechanicalsoup
import json
import datetime
import time
import logging
import configparser
import sys

logging.basicConfig(filename='NewsCrawler.log',level=logging.INFO,format='%(asctime)s %(threadName)s %(message)s')
config = configparser.ConfigParser()
config.read("newscrawler.conf")
start_date_year = config.getint('nytimes','start_date_year')
start_date_month = config.getint('nytimes','start_date_month')
start_date_day = config.getint('nytimes','start_date_day')
end_date_year = config.getint('nytimes','end_date_year')
end_date_month = config.getint('nytimes','end_date_month')
end_date_day = config.getint('nytimes','end_date_day')


def crawl_nytimes_archive(queue):
	
	browser = mechanicalsoup.Browser()

	#Define Date Values so they can be incremented in search URL
	end_date = datetime.date(end_date_year,end_date_month,end_date_day)
	#Defines the date to start crawling at.
	target_date = datetime.date(start_date_year,start_date_month,start_date_day)

	base_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20160319&begin_date=20070101&facet=true"
	paged_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20160319&begin_date=20070101&page=1&facet=true"
	
	#Loop through search results by calling search URL for each date in range one at a time
	while target_date < end_date:
		search_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date={0:d}{1:02d}{2:02d}&begin_date={3:d}{4:02d}{5:02d}&page=1&facet=true".format(target_date.year,target_date.month,target_date.day,target_date.year,target_date.month,target_date.day)
		print(search_url)
		date_start_time = time.time()
		
		page_number = 1
		#Loop through each page of search results
		while page_number < 100:
			page_start_time = time.time()
			logging.info("Base nytimes Search URL = {}".format(search_url))
			#Raise exception if network IO fails but continue execution
			#Causes program to survice a command line Ctrl-C, would like to profile network error and catch specifically instead
			try:
				search_page = browser.get(search_url)
			except:
				e = sys.exc_info()[0]
				logging.info("nytimes queueing Error: {}".format(e))
				break

			response = search_page.json()['response']
			for snippet in response['docs']:
				#Place article link on queue
				logging.info('Queueing url {}'.format(snippet['web_url']))
				logging.info('Queuing following variables: {}, {}, {}'.format(snippet['web_url'],"nytimes",target_date))
				queue.put((snippet['web_url'],"nytimes",target_date))

			print("Day {} Page number {} successful".format(target_date.strftime('%m-%d-%Y'),page_number))
			#Get new page number and preserve old to perform string replacement in new url.
			old_page_number = page_number
			page_number += 1
			search_url = search_url[::-1].replace(str(old_page_number)[::-1],str(page_number)[::-1],1)[::-1]

			logging.info("Day {} Page {} queued in {} seconds".format(target_date,old_page_number,(time.time() - page_start_time)))
		
		logging.info("Day {} queued in {} seconds".format(target_date,(time.time() - date_start_time)))
		#Increment date to search new url when all pages for last date are complete. 
		target_date += datetime.timedelta(days=1)