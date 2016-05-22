import unittest
import json
from crawlers import nytimes
# import mechanicalsoup
from unittest.mock import MagicMock, patch
import requests
from retrying import retry
import os
from queue import Queue
import datetime
import logging
import responses

class TestNYTimes(unittest.TestCase):

	browser = nytimes.mechanicalsoup.Browser()
	search_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20070101&begin_date=20070101&page=1&facet=true"
	first_day_articles = ['http://www.nytimes.com/2007/01/01/sports/ncaafootball/01cnd-outback.html','http://www.nytimes.com/2007/01/01/world/europe/01cnd-union.html','http://www.nytimes.com/2007/01/01/world/asia/01cnd-thai.html','http://www.nytimes.com/2007/01/01/nyregion/01cnd-stext.html','http://www.nytimes.com/2007/01/01/nyregion/01cnd-eliot.html','http://www.nytimes.com/2007/01/01/nyregion/01mbrfs-ONEISKILLEDI_BRF.html','http://www.nytimes.com/2007/01/01/sports/football/01packers.html','http://www.nytimes.com/2007/01/01/nyregion/01mbrfs-burn.html','http://www.nytimes.com/2007/01/01/nyregion/01mbrfs-transit.html','http://www.nytimes.com/2007/01/01/nyregion/01mbrfs-crash.html']
	target_date = datetime.date(2007,1,1)	
	stub_queuedElements = []

	for i in range(0,len(first_day_articles)):
		stub_queuedElements.append((first_day_articles[i],'nytimes',target_date))

	with open(os.path.join(os.path.abspath(os.path.dirname(__file__)),'ResponseContent.json')) as f:
		response = f.read()
		responseJson = json.dumps(response)
	print(type(responseJson))

	maxDiff = None

	def setUp(self):
		logging.disable(logging.CRITICAL)

	def test_goodresponse_getSearchJSON(self):
		response = nytimes.getSearchJSON(self.browser,self.search_url)
		self.assertEqual(200,response.status_code)


	@patch('mechanicalsoup.Browser.get')
	def test_backoff_getSearchJSON(self,mock_get):
		mock_get.side_effect = [requests.exceptions.ConnectionError("Connection Error"),requests.exceptions.ReadTimeout("ReadTimeout"),MagicMock(status_code=200, headers={'content-type':"application/json"},
                         text=json.dumps({'status':True}))]
		self.maxDiff = None
		response = nytimes.getSearchJSON(self.browser,self.search_url)
		self.assertEqual(200,response.status_code)
	
	@patch('requests.models.Response.json')
	def test_parseSearchJSON(self,mock_json):
		mock_json.json = self.response
		mock_response = MagicMock(spec=requests.models.Response,status_code=200, body=self.responseJson, json=self.responseJson)
		print("mock_response is type " + str(type(mock_response)))
		self.maxDiff = None
		#Not really a unit test, since calls another function, but very difficult to mock and will reveal errors anyway
		article_urls = nytimes.parseSearchJSON(mock_json)
		self.assertEqual(self.first_day_articles,article_urls)

	def test_queueArticles(self):
		queueArticlesQueue = Queue()

		nytimes.queueArticles(self.first_day_articles,queueArticlesQueue,self.target_date)
		self.assertEqual(len(self.first_day_articles),queueArticlesQueue.qsize())
		self.maxDiff = None

		queuedElements = []
				
		while queueArticlesQueue.empty() is not True:
			queuedElements.append(queueArticlesQueue.get())
			
		#Assert that the lists have the same elements though they may not be in the same order
		self.assertCountEqual(queuedElements,self.stub_queuedElements)

	def test_getConfig(self):
		start_date_year, start_date_month, start_date_day, end_date_year, end_date_month, end_date_day = nytimes.getConfig()
		assert isinstance(start_date_year, int)
		assert isinstance(start_date_month, int)
		assert isinstance(start_date_day, int)
		assert isinstance(end_date_year, int)
		assert isinstance(end_date_month, int)
		assert isinstance(end_date_day, int)

	def test_crawlPage(self):
		crawlPageQueue = Queue()
		
		nytimes.crawlPage(self.target_date, self.browser, self.search_url, crawlPageQueue, 1)
		self.assertEqual(crawlPageQueue.qsize(),10)
		queuedElements = []
				
		while crawlPageQueue.empty() is not True:
			queuedElements.append(crawlPageQueue.get())

		self.assertCountEqual(queuedElements,self.stub_queuedElements)

	@patch('mechanicalsoup.Browser.get')
	def test_exceptions_crawlPage(self,mock_get):
		testExceptionsQueue = Queue()
		mock_get.side_effect = [requests.exceptions.MissingSchema("Missing Schema Error"),sentinel.DEFAULT,sentinel.DEFAULT]

		test_searches = ['http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=&begin_date=13132007&page=1&facet=true','http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20070101&begin_date=20070101&page=1&facet=',self.search_url]

		for search_url in test_searches:
			result = nytimes.crawlPage(self.target_date, self.browser, search_url, testExceptionsQueue, 1)
		self.assertEqual(len(self.first_day_articles),testExceptionsQueue.qsize())

		queuedElements = []
				
		while testExceptionsQueue.empty() is not True:
			queuedElements.append(testExceptionsQueue.get())

		self.assertCountEqual(queuedElements,self.stub_queuedElements)

	@patch('mechanicalsoup.Browser.get')
	def test_missingSchemaException_crawlPage(self,mock_get):
		missingSchemaQueue = Queue()
		mock_get.side_effect = requests.exceptions.MissingSchema("Missing Schema Error")
		nytimes.crawlPage(self.target_date, self.browser, self.search_url, missingSchemaQueue, 1)


if __name__ == '__main__':
    unittest.main()
