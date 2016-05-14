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
	search_url = "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=01012007&begin_date=01012007&page=1&facet=true"
	first_day_articles = ['http://www.nytimes.com/2016/05/18/dining/phyllo-torte-recipe-ricotta.html', 'http://www.nytimes.com/2016/05/18/dining/dandelion-salad-recipe.html', 'http://www.nytimes.com/2016/05/18/dining/single-malt-whiskey-review.html', 'http://www.nytimes.com/2016/05/18/dining/taiwan-bear-house-hungry-city.html', 'http://www.nytimes.com/slideshow/2016/05/18/dining/taiwan-bear-house.html', 'http://www.nytimes.com/2016/05/17/science/emily-dickinson-lost-gardens.html', 'http://www.nytimes.com/2016/05/16/sports/two-hour-marathon-kenenisa-bekele.html', 'http://www.nytimes.com/2016/05/12/sports/soccer/chan-yuen-ting-female-soccer-coach-hong-kong.html', 'http://cooking.nytimes.com/recipes/1018114-warm-kale-coconut-and-tomato-salad', 'http://www.nytimes.com/2016/05/15/arts/television/all-the-way-hbo.html']
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

	# def test_main(self):
	# 	nytimes.crawl_nytimes_archive(self.queue)

if __name__ == '__main__':
    unittest.main()
