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

	def test_DownloadWorker(self):
		
		# mock_parse.return_value = [crawler.newspaper.article.ArticleException]
		# print(str(mock_parse.call_count))
		##This Works!!! It replaces the parse function that DownloadWorker calls. Need logic to continue testing.
		crawler.parsearticle.parse = MagicMock(side_effect=[crawler.newspaper.article.ArticleException,])
		queue = crawler.Queue()
		worker = crawler.DownloadWorker(queue)
		# print(dir(crawler))
		# worker.run.parsearticle.parse = MagicMock(side_effect=[crawler.newspaper.article.ArticleException])
		worker.start()
		# print(dir(crawler.parsearticle.getArticle))
		queue.put(("BadURL","test","20070101"))
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
