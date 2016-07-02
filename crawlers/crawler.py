from queue import Queue
from threading import Thread
import time
import logging
import configparser
import newspaper
import os

from crawlers import parsearticle 
from crawlers import nytimes

root_path = os.path.abspath(os.path.dirname(__file__))
log_path = root_path+"/../logs"
if not os.path.exists(log_path):
		os.makedirs(log_path)

#Set program to log to a file
#Configure the root logger for application. 
crawl_logger = logging.basicConfig(filename=log_path+'/news_crawler.log',level=logging.INFO,format='%(asctime)s %(threadName)s %(levelname)s: %(message)s')

#Configure a separate logger specifically for recording anomoly events in the parser code
parse_ledger = logging.getLogger('parse_ledger')
parse_ledger.setLevel(logging.DEBUG)
parse_ledger.propagate = False
parse_handler = logging.FileHandler(filename=log_path+"/parse_ledger.log")
parse_handler.setLevel(logging.DEBUG)

parse_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
parse_handler.setFormatter(parse_formatter)

parse_ledger.addHandler(parse_handler)

stats_logger = logging.getLogger('stats_logger')
stats_logger.setLevel(logging.DEBUG)
stats_logger.propagate = False
stats_handler = logging.FileHandler(filename=log_path+"/"+"run_stats.log")
stats_handler.setLevel(logging.DEBUG)
stats_formatter = logging.Formatter('%(asctime)s,%(threadName)s,%(message)s')
stats_handler.setFormatter(stats_formatter)
stats_logger.addHandler(stats_handler)

#Create conf file from template if not done already
if not os.path.exists(os.path.join(root_path,"newscrawler.conf")):
	os.symlink(os.path.join(root_path,"newscrawler.conf"),os.path.join(root_path,"newscrawler.conf.template"))
#Get runtime configuration values
config = configparser.ConfigParser()
config.read(root_path+"/newscrawler.conf")
numThreads = config.getint('crawler','threads') 
#Collect comma separated list of newsSources
newsSourceList = config.get('crawler','newsSources').split(',')

#Configure separate logger for link crawlers error output.
#Child modules should call crawl_ledger = logging.getLogger('crawl_ledger')
crawl_ledger = logging.getLogger('crawl_ledger')
crawl_ledger.setLevel(logging.DEBUG)
crawl_ledger.propagate = False
crawl_handler = logging.FileHandler(filename=log_path+"/crawl_ledger.log")
crawl_handler.setLevel(logging.DEBUG)
crawl_handler.setFormatter(parse_formatter)
crawl_ledger.addHandler(crawl_handler)



class DownloadWorker(Thread):
	
	def __init__(self, queue, metadataQueue):
		Thread.__init__(self)
		self.queue = queue
		self.metadataQueue = metadataQueue

	def run(self):
		logging.info("DownloadWorker started ")
		i = 0
		worker_start = time.time()
		while True:
			url, source, urlDate = self.queue.get()
			task_start = time.time()
			#Use modulo counter to print aggregated runtime stats
			if i % 100 == 0:
				logging.info("{} articles parsed in {} seconds".format(i,time.time()-worker_start))
			try:
						
				parsearticle.parse(url, source, urlDate, self.metadataQueue)
				parse_ledger.debug("{} parsed successfully".format(url))

			except newspaper.article.ArticleException as e:
				# print("I see an exception")
				logging.exception("Exception trying to parse article at url {}".format(url))
				parse_ledger.warning("{} skipped, newspaper.article.ArticleException".format(url))
				pass
			
			except FileExistsError:
				logging.warning("Skipped fullTextWrite and metadata queueing for duplicate filename on {} article at url {}".format(source,url))
				parse_ledger.warning("{} skipped, duplicate in fullText directory".format(url))
				pass

			self.queue.task_done()
			i+=1
			

class MetadataWriterWorker(Thread):
	
	def __init__(self, metadataQueue,newsSource):
		Thread.__init__(self)
		self.metadataQueue = metadataQueue
		self.newsSource = newsSource

	def run(self):
		logging.info("MetadataWriterWorker started for {}".format(self.newsSource))
		
		parsearticle.writeMetadataHeader(self.newsSource)  
		
		while True:
			title, date, url, authors, newsSource, article_text_filename = self.metadataQueue.get()
			logging.debug("MetadataWriterWorker got from queue url {}".format(url))
			parsearticle.writeMetadataRow(title, date, url, authors, newsSource, article_text_filename)
			self.metadataQueue.task_done()

class NewsCrawler():

	def __init__(self,newsSource, numThreads):
		self.newsSource = newsSource
		self.linksQueue = Queue()
		self.metadataQueue = Queue()
		self.numThreads = numThreads

	def crawl(self):
		for x in range(self.numThreads):
			worker = DownloadWorker(self.linksQueue,self.metadataQueue)
			worker.daemon = True
			worker.start()

		#This section maintains a list of available news sources	
		if self.newsSource == 'nytimes':
			self.start_metadata_worker()
			nytimes.crawl_nytimes_archive(self.linksQueue)
			self.wait_for_crawl_completion()
		else:
			logging.exception("Crawler doesn't recognize news source: {}".format(self.newsSource))

	def start_metadata_worker(self):
		metadataWorker = MetadataWriterWorker(self.metadataQueue,self.newsSource)
		metadataWorker.daemon = True
		metadataWorker.start()

	def wait_for_crawl_completion(self):
		self.linksQueue.join()
		self.metadataQueue.join()

def main():
	logging.info("Crawl Run Beginning")
	ts = time.time()
	#Create queue to communicate with worker threads
	# queue = Queue()
	# metadataQueue = Queue()
	#Create numThreads Worker Threads to download files and write out their fulltext
	# for x in range(numThreads):
	# 	worker = DownloadWorker(queue,metadataQueue)
	# 	worker.daemon = True
	# 	worker.start()

	#Call source specific crawler(s) to enqueue tasks

	# for newsSource in newsSourceList:
	# 	if newsSource == 'nytimes':
	# 		nytimes.crawl_nytimes_archive(queue)
	# 	else:
	# 		logging.exception("Crawler doesn't recognize news source: {}".format(newsSource))
		
	# 	#Create a metadataWorker thread for each active newsSource
	# 	metadataWorker = MetadataWriterWorker(metadataQueue,newsSource)
	# 	metadataWorker.daemon = True
	# 	metadataWorker.start()

	# queue.join()
	# #Adding so program doesn't stop writing once link queue is done, but continues till all queued items are handled.
	# metadataQueue.join()
	for newsSource in newsSourceList:
		print("Started crawler for {}".format(newsSource))
		
		crawler = NewsCrawler(newsSource,numThreads)
		crawler.crawl()

	logging.info('Took {} seconds to complete crawler run'.format(time.time() -ts))



if __name__ == '__main__':
   main()