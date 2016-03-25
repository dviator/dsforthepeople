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

def extract_links_from_search_results_json(search_page):
	response = search_page.json()['response']
	for snippet in response['docs']:
		print(snippet['web_url'])
		parse(snippet['web_url'])
	

def crawl_search_pages():
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
		begin_date += datetime.timedelta(days=1)
		page_number = 1
		#Placeholder while loop until more efficient loop breaker can be found
		while page_number < 100:

			print("Search URL = ",search_url)
			search_page = browser.get(search_url)
			extract_links_from_search_results_json(search_page)
			print("Page number ",page_number,"successful")
			old_page_number = page_number
			page_number += 1
		
			search_url = search_url[::-1].replace(str(old_page_number)[::-1],str(page_number)[::-1],1)[::-1]
			


	#Implement DEBUG Logging Here
	# print(type(search_results))
	# print(search_results.keys())
	# print(search_results['response'])

	# #print(search_page_one.json())
	# print(type(search_page_one.json()))
	# print 
crawl_search_pages()


	