#Take raw http address
#Fetch HTML
#Parse handful of article links from HTML
#Pass to newspaper class
#Collect relevant data in newspaper class
#Iterate search page number in URL string
#Repeat

import mechanicalsoup
import json
import datetime
from parseArticle import parse
import time
import logging


#Unnecessary with Multithreaded implementation. Logic absorbed into crawl nytimes_archive function
# def extract_links_from_search_results_json(search_page):
# 	response = search_page.json()['response']
# 	return map(lambda item: )
# 	for snippet in response['docs']:
# 		print(snippet['web_url'])

		# parse_start = time.time()
		# parse(snippet['web_url'],"nytimes")
		# logging.info("Parse call completed in {} seconds".format(time.time() - parse_start))
	

def crawl_nytimes_archive(queue):
	logging.basicConfig(filename='NewsCrawler.log',level=logging.INFO,format='%(asctime)s %(threadName)s %(message)s')
	browser = mechanicalsoup.Browser()
	#Write an outer loop that increments the dates in the URL's
	#Turn this into a loop of page visits

	#Define Date Values so they can be incremented in search URL
	end_date = datetime.date(2016,3,19)
	begin_date = datetime.date(2007,1,1)

	base_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20160319&begin_date=20070101&facet=true"
	paged_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20160319&begin_date=20070101&page=1&facet=true"
	
	
	while begin_date < end_date:
		search_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date={0:d}{1:02d}{2:02d}&begin_date={3:d}{4:02d}{5:02d}&page=1&facet=true".format(begin_date.year,begin_date.month,begin_date.day,begin_date.year,begin_date.month,begin_date.day)
		print(search_url)
		date_start_time = time.time()
		begin_date += datetime.timedelta(days=1)
		page_number = 1
		#Placeholder while loop until more efficient loop breaker can be found
		while page_number < 100:
			page_start_time = time.time()
			logging.info("Base nytimes Search URL = {}".format(search_url))
			search_page = browser.get(search_url)
			response = search_page.json()['response']
			for snippet in response['docs']:
				#print(snippet['web_url'])
				logging.info('Queueing url {}'.format(snippet['web_url']))
				#Place article link on queue
				queue.put((snippet['web_url'],"nytimes"))
			
			#Shutting off for multithreading implementation experiment
			# extract_links_from_search_results_json(search_page)

			print("Page number ",page_number,"successful")
			old_page_number = page_number
			page_number += 1
		
			search_url = search_url[::-1].replace(str(old_page_number)[::-1],str(page_number)[::-1],1)[::-1]
			logging.info("Day {} Page {} queued in {} seconds".format(begin_date,old_page_number,(time.time() - page_start_time)))
		
		logging.info("Day {} queued in {} seconds".format(begin_date,(time.time() - date_start_time)))
		

	#Implement DEBUG Logging Here
	# print(type(search_results))
	# print(search_results.keys())
	# print(search_results['response'])

	# #print(search_page_one.json())
	# print(type(search_page_one.json()))
	# print 
# crawl_nytimes_archive()


	