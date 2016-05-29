import unittest
from unittest.mock import MagicMock, patch
import retrying
import logging
from crawlers import crawler

class TestCrawler(unittest.TestCase):

	


	def setUp(self):
		logging.disable(logging.CRITICAL)

	@patch('crawlers.crawler.nytimes.crawl_nytimes_archive')
	def test_nytimes_called(self,mock_crawl_nytimes_archive):
		crawler.main()
		assert mock_crawl_nytimes_archive.called

	def test_DownloadWorker_passes_exceptions(self):
		#Call the function with exceptions and with good ouput to ensure it continues execution
		crawler.parsearticle.parse = MagicMock(side_effect=[crawler.newspaper.article.ArticleException,None,crawler.newspaper.article.ArticleException,crawler.newspaper.article.ArticleException])
		queue = crawler.Queue()
		worker = crawler.DownloadWorker(queue)

		#Call the worker as a daemon so that it exits when the test case that called it exits.
		worker.daemon = True
		worker.start()

		#Put 4 entries on the queue
		for _ in range(4):
			queue.put(("BadURL","test","20070101"))
		#Wait until the queue is empty to stop execution
		queue.join()
		#Make sure a call was made to parse for each entry in the queue
		assert crawler.parsearticle.parse.call_count == 4

	# @patch('crawlers.crawler.DownloadWorker.start')
	# @patch('crawlers.crawler.parsearticle.parse')
	# @patch('crawlers.crawler.nytimes.crawl_nytimes_archive')
	# def test_DownloadWorkers_spawned(self,mock_DownloadWorker,mock_parse, mock_crawl_nytimes_archive):
	# 	side_effects = []
	# 	for _ in range(30):
	# 		side_effects.append(MagicMock)
	# 	mock_DownloadWorker.side_effect = side_effects
	# 	crawler.main()
	# 	print(dir(crawler.DownloadWorker))
	# 	print(dir(mock_DownloadWorker))
	# 	print("NumThreads="+str(crawler.numThreads))
	# 	print(mock_DownloadWorker.call_count)
