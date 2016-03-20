#Take raw http address
#Fetch HTML
#Parse handful of article links from HTML
#Pass to newspaper class
#Collect relevant data in newspaper class
#Iterate search page number in URL string
#Repeat

import mechanicalsoup
import json
import pprint

def extract_links_from_search_results_json(search_page):
	response = search_page.json()['response']
	for snippet in response['docs']:
		print(snippet['web_url'])
	

def crawl_search_pages():
	browser = mechanicalsoup.Browser()
	#Write an outer loop that increments the dates in the URL's
	#Turn this into a loop of page visits
	base_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20160319&begin_date=20070101&facet=true"
	paged_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20160319&begin_date=20070101&page=1&facet=true"
	page_number = 0 
	while True:
		print("Base URL = ",base_url)
		search_page = browser.get(base_url)
		extract_links_from_search_results_json(search_page)
		page_number += 1
		base_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20160319&begin_date=20070101&page=" + str(page_number) + "&facet=true"
		print("Page number ", page_number,"successful")
	#Implement DEBUG Logging Here
	# print(type(search_results))
	# print(search_results.keys())
	# print(search_results['response'])

	# #print(search_page_one.json())
	# print(type(search_page_one.json()))
	# print 
crawl_search_pages()


	