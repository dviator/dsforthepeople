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

class TestNYTimes(unittest.TestCase):

	browser = nytimes.mechanicalsoup.Browser()
	search_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=20070101&begin_date=20070101&page=1&facet=true"
	first_day_articles = ['http://www.nytimes.com/2007/01/01/sports/ncaafootball/01cnd-outback.html','http://www.nytimes.com/2007/01/01/world/europe/01cnd-union.html','http://www.nytimes.com/2007/01/01/world/asia/01cnd-thai.html','http://www.nytimes.com/2007/01/01/nyregion/01cnd-stext.html','http://www.nytimes.com/2007/01/01/nyregion/01cnd-eliot.html','http://www.nytimes.com/2007/01/01/nyregion/01mbrfs-ONEISKILLEDI_BRF.html','http://www.nytimes.com/2007/01/01/sports/football/01packers.html','http://www.nytimes.com/2007/01/01/nyregion/01mbrfs-burn.html','http://www.nytimes.com/2007/01/01/nyregion/01mbrfs-transit.html','http://www.nytimes.com/2007/01/01/nyregion/01mbrfs-crash.html']
	target_date = datetime.date(2007,1,1)	
	stub_queuedElements = []

	for i in range(0,len(first_day_articles)):
		stub_queuedElements.append((first_day_articles[i],'nytimes',target_date))

	with open(os.path.join(os.path.abspath(os.path.dirname(__file__)),'ResponseContent.json')) as f:
		responseContent = json.load(f)
		response = f.read()

	maxDiff = None
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

	def test_parseSearchJSON(self):
		mock_response = MagicMock(status_code=200, text=self.response)
		self.maxDiff = None
		#Not really a unit test, since calls another function, but very difficult to mock and will reveal errors anyway
		article_urls = nytimes.parseSearchJSON(nytimes.getSearchJSON(self.browser,self.search_url))
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

	def test_exceptions_crawlPage(self):
		testExceptionsQueue = Queue()
		mock_searches = MagicMock(side_effect=['http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=&begin_date=13132007&page=1&facet=true'])

		nytimes.crawlPage(self.target_date, self.browser, badSearchURL, testExceptionsQueue, 1)



if __name__ == '__main__':
    unittest.main()
