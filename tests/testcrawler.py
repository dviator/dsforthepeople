import unittest
from unittest.mock import MagicMock, patch
from crawlers import crawler

class TestCrawler(unittest.TestCase):


	@patch('crawlers.crawler.nytimes.crawl_nytimes_archive')
	def test_nytimes_called(self,mock_crawl_nytimes_archive):
		crawler.main()
		assert mock_crawl_nytimes_archive.called

	@patch('crawlers.crawler.DownloadWorker.start')
	@patch('crawlers.crawler.parsearticle.parse')
	@patch('crawlers.crawler.nytimes.crawl_nytimes_archive')
	def test_DownloadWorkers_spawned(self,mock_DownloadWorker,mock_parse, mock_crawl_nytimes_archive):
		side_effects = []
		for _ in range(30):
			side_effects.append(MagicMock)
		mock_DownloadWorker.side_effect = side_effects
		crawler.main()
		print(dir(crawler.DownloadWorker))
		print(dir(mock_DownloadWorker))
		print("NumThreads="+str(crawler.numThreads))
		print(mock_DownloadWorker.call_count)