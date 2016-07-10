import unittest
from unittest.mock import MagicMock, patch
import retrying
import logging
from crawlers import crawler
import time
import requests

class TestCrawler(unittest.TestCase):

	dummyMetadata = ("DummyData1","DummyData2","DummyData3","DummyData4","DummyData5","DummyData6")
	dummyLinkData = ("BadURL","test","20070101")
	def setUp(self):
		logging.disable(logging.CRITICAL)

	@patch('crawlers.crawler.nytimes.crawl_nytimes_archive')
	def test_main_calls_correct_link_generators(self,mock_crawl_nytimes_archive):
		crawler.main()
		assert mock_crawl_nytimes_archive.called

	@patch('crawlers.crawler.parsearticle.parse')
	def test_DownloadWorker_bypasses_ArticleException_and_FileExistsError(self,mock_parse):
		#Call the function with exceptions and with good ouput to ensure it continues execution
		# crawler.parsearticle.parse = MagicMock(side_effect=[crawler.newspaper.article.ArticleException,FileExistsError,self.dummyMetadata])
		# crawler.parsearticle.parse.start()
		mock_parse.side_effect = [crawler.newspaper.article.ArticleException,FileExistsError,self.dummyMetadata]
		queue = crawler.Queue()
		metadataQueue = crawler.Queue()
		worker = crawler.DownloadWorker(queue,metadataQueue)

		#Call the worker as a daemon so that it exits when the test case that called it exits.
		worker.daemon = True
		worker.start()

		#Put 4 entries on the queue
		for _ in range(3):
			queue.put(self.dummyLinkData)
		#Wait until the queue is empty to stop execution
		queue.join()

		#Unmock the method to return it to normal for other tests		
		# crawler.parsearticle.parse.stop()
		#Make sure a call was made to parse for each entry in the queue
		assert crawler.parsearticle.parse.call_count == 3
		#Make sure each entry in the queue was processed and the call to queue.task_done was made
		assert queue.qsize() == 0

	@patch('crawlers.crawler.parsearticle.parse')
	def test_DownloadWorker_bypasses_HTTPError_exception(self,mock_parse):
		mock_parse.side_effect = [requests.exceptions.HTTPError,self.dummyMetadata]
		queue = crawler.Queue()
		metadataQueue = crawler.Queue()
		worker = crawler.DownloadWorker(queue,metadataQueue)

		#Call the worker as a daemon so that it exits when the test case that called it exits.
		worker.daemon = True
		worker.start()

		#Put 4 entries on the queue
		for _ in range(2):
			queue.put(self.dummyLinkData)
		#Wait until the queue is empty to stop execution
		queue.join()

		assert crawler.parsearticle.parse.call_count == 2

		assert queue.qsize() == 0 


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

		metadataQueue.join()
		print("Metadata headers calls count = {}".format(crawler.parsearticle.writeMetadataHeader.call_count))
		assert crawler.parsearticle.writeMetadataHeader.call_count == 1
		assert crawler.parsearticle.writeMetadataRow.call_count == 3
		#Fetches metadata fields from a queue
		#Writes them to a file

	@patch('crawlers.crawler.parsearticle.getArticle')
	def test_crawler_passes_bad_getArticle(self,mock_getArticle):
		mock_getArticle.side_effect = [crawler.newspaper.article.ArticleException]
		queue = crawler.Queue()
		metadataQueue = crawler.Queue()
		worker = crawler.DownloadWorker(queue,metadataQueue)

		#Call the worker as a daemon so that it exits when the test case that called it exits.
		worker.daemon = True
		worker.start()

		
		queue.put(self.dummyLinkData)
		# queue.put(self.dummyLinkData)
		# queue.put(("/data/daily/2006/11/30/778907.sgml","test","20070101"))
		# queue.put(self.dummyLinkData)
		#Wait until the queue is empty to stop execution
		queue.join()


		# Make sure a call was made to parse for each entry in the queue
		assert mock_getArticle.call_count == 1
		# Make sure each entry in the queue was processed and the call to queue.task_done was made
		assert queue.qsize() == 0


	#Need to test that the main program properly instantiates the metadataWriterWorker
	#This includes proper queues and later on, 1 thread per newsSource
	#Commenting out as moved queuing function into parsearticle.
	#Crawler now just calls parse and passes it the metadataQueue object, and trust parse to complete the queuing or return an error.
	# def test_DownloadWorker_returns_metadata(self):
	# 	queue = crawler.Queue()
	# 	metadataQueue = crawler.Queue()

	# 	crawler.parsearticle.parse = MagicMock(return_value=self.dummyMetadata)
	# 		#Call the worker as a daemon so that it exits when the test case that called it exits.
	# 	worker = crawler.DownloadWorker(queue,metadataQueue)
	# 	worker.daemon = True
	# 	worker.start()
	# 	# print(metadataQueue.qsize())
	# 	#Wait until the queue is empty to stop execution
	# 	queue.put(self.dummyLinkData)
		
	# 	testedMetadataRow = metadataQueue.get()
	# 	# print(testedMetadataRow)
	# 	queue.join()

	# 	self.assertEqual(self.dummyMetadata,testedMetadataRow)