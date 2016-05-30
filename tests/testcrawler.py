import unittest
from unittest.mock import MagicMock, patch
import retrying
import logging
from crawlers import crawler

class TestCrawler(unittest.TestCase):

	dummyMetadata = ("DummyData1","DummyData2","DummyData3","DummyData4","DummyData5","DummyData6")
	dummyLinkData = ("BadURL","test","20070101")
	def setUp(self):
		logging.disable(logging.CRITICAL)

	@patch('crawlers.crawler.nytimes.crawl_nytimes_archive')
	def test_main_calls_link_generator_nytimes(self,mock_crawl_nytimes_archive):
		crawler.main()
		assert mock_crawl_nytimes_archive.called

	def test_DownloadWorker_passes_exceptions(self):
		#Call the function with exceptions and with good ouput to ensure it continues execution
		crawler.parsearticle.parse = MagicMock(side_effect=[crawler.newspaper.article.ArticleException,self.dummyMetadata,crawler.newspaper.article.ArticleException,crawler.newspaper.article.ArticleException])
		crawler.parsearticle.parse.start()
		queue = crawler.Queue()
		metadataQueue = crawler.Queue()
		worker = crawler.DownloadWorker(queue,metadataQueue)

		#Call the worker as a daemon so that it exits when the test case that called it exits.
		worker.daemon = True
		worker.start()

		#Put 4 entries on the queue
		for _ in range(4):
			queue.put(self.dummyLinkData)
		#Wait until the queue is empty to stop execution
		queue.join()
		crawler.parsearticle.parse.stop()
		#Make sure a call was made to parse for each entry in the queue
		assert crawler.parsearticle.parse.call_count == 4


	#Test that the metadatawriter worker properly calls it's parsearticle write methods
	def test_metadataWriterWorker_calls_header_and_row_writes(self):
		crawler.parsearticle.writeMetadataHeader = MagicMock()
		crawler.parsearticle.writeMetadataRow = MagicMock()

		metadataQueue = crawler.Queue()
		for _ in range(3):
			metadataQueue.put(self.dummyMetadata)

		metadataWorker = crawler.MetadataWriterWorker(metadataQueue,"test")
		metadataWorker.daemon = True
		metadataWorker.start()

		assert crawler.parsearticle.writeMetadataHeader.call_count == 1
		assert crawler.parsearticle.writeMetadataRow.call_count == 3
		#Fetches metadata fields from a queue
		#Writes them to a file

	#Need to test that the main program properly instantiates the metadataWriterWorker
	#This includes proper queues and later on, 1 thread per newsSource
	def test_DownloadWorker_returns_metadata(self):
		queue = crawler.Queue()
		metadataQueue = crawler.Queue()

		crawler.parsearticle.parse = MagicMock(return_value=self.dummyMetadata)
			#Call the worker as a daemon so that it exits when the test case that called it exits.
		worker = crawler.DownloadWorker(queue,metadataQueue)
		worker.daemon = True
		worker.start()
		# print(metadataQueue.qsize())
		#Wait until the queue is empty to stop execution
		queue.put(self.dummyLinkData)
		
		testedMetadataRow = metadataQueue.get()
		# print(testedMetadataRow)
		queue.join()
		self.assertEqual(self.dummyMetadata,testedMetadataRow)