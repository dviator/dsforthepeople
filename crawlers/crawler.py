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
#Set program to log to a file
#Configure the root logger for application. 
crawl_logger = logging.basicConfig(filename=log_path+'/news_crawler.log',level=logging.INFO,format='%(asctime)s %(threadName)s %(levelname)s: %(message)s')

#Configure loggersc
# crawl_logger = logging.getLogger(__name__)
# crawl_logger.setLevel(logging.DEBUG)
# crawl_logger.propagate = False
# crawl_handler = logging.FileHandler(filename=root_path+'/TestNewsCrawler.log')
# crawl_handler.setLevel(logging.DEBUG)

# formatter = logging.Formatter('%(asctime)s %(threadName)s %(levelname)s: %(message)s')

# crawl_handler.setFormatter(formatter)
# crawl_logger.addHandler(crawl_handler)

#Configure a separate logger specifically for recording anomoly events in the parser code
parse_ledger = logging.getLogger('parse_ledger')
parse_ledger.setLevel(logging.INFO)
parse_ledger.propagate = False
parse_handler = logging.FileHandler(filename=log_path+"/parse_ledger.log")
parse_handler.setLevel(logging.DEBUG)

parse_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
parse_handler.setFormatter(parse_formatter)

parse_ledger.addHandler(parse_handler)

#Get runtime configuration values
config = configparser.ConfigParser()
config.read(root_path+"/newscrawler.conf")
numThreads = config.getint('crawler','threads') 
newsSource = config.get('crawler','newsSource')

#Child modules should call crawl_ledger = logging.getLogger('crawl_ledger')
crawl_ledger = logging.getLogger('crawl_ledger')
crawl_ledger.setLevel(logging.DEBUG)
crawl_ledger.propagate = False
crawl_handler = logging.FileHandler(filename=log_path+"/"+newsSource+"_ledger.log")
crawl_handler.setLevel(logging.DEBUG)
crawl_handler.setFormatter(parse_formatter)
crawl_ledger.addHandler(crawl_handler)

stats_logger = logging.getLogger('stats_logger')
stats_logger.setLevel(logging.DEBUG)
stats_logger.propagate = False
stats_handler = logging.FileHandler(filename=log_path+"/"+"run_stats.log")
stats_handler.setLevel(logging.DEBUG)
stats_formatter = logging.Formatter('%(asctime)s,%(threadName)s,%(message)s')
stats_handler.setFormatter(stats_formatter)
stats_logger.addHandler(stats_handler)

class DownloadWorker(Thread):
	
	def __init__(self, queue, metadataQueue):
		Thread.__init__(self)
		self.queue = queue
		self.metadataQueue = metadataQueue

	def run(self):
		i = 0
		worker_start = time.time()
		while True:
			url, source, urlDate = self.queue.get()
			task_start = time.time()
			#Use modulo counter to print aggregated runtime stats
			if i % 100 == 0:
				logging.info("{} articles parsed in {} seconds".format(i,time.time()-worker_start))
			try:
				
				# title, date, url, authors, newsSource, article_text_filename = parsearticle.parse(url, source, urlDate)
				parsearticle.parse(url, source, urlDate, self.metadataQueue)
				parse_ledger.debug("{} parsed successfully".format(url))
				# print("I got from parse {},{},{},{},{},{}".format(title,date,url,authors,newsSource,article_text_filename))
				# self.metadataQueue.put((title, date, url, authors, newsSource, article_text_filename))
				# print(self.metadataQueue.qsize())

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
		parsearticle.writeMetadataHeader(self.newsSource)
		while True:
			title, date, url, authors, newsSource, article_text_filename = self.metadataQueue.get()
			parsearticle.writeMetadataRow(title, date, url, authors, newsSource, article_text_filename)
			self.metadataQueue.task_done()


def main():
	ts = time.time()
	#Create queue to communicate with worker threads
	queue = Queue()
	metadataQueue = Queue()
	#Create numThreads Worker Threads to download files and write out their fulltext
	for x in range(numThreads):
		worker = DownloadWorker(queue,metadataQueue)
		worker.daemon = True
		worker.start()

	#Create a single metadataWorker to write metadata in a single thread as articles are donwloaded
	metadataWorker = MetadataWriterWorker(metadataQueue,newsSource)
	metadataWorker.daemon = True
	metadataWorker.start()
	#Will need a loop around this if there are to be multiple newsSource running concurrently.

	#Call source specific crawler(s) to enqueue tasks
	logging.info("Begin queueing links")
	nytimes.crawl_nytimes_archive(queue)

	#Start metadata
	queue.join()
	#Adding so program doesn't stop writing once link queue is done, but continues till all queued items are handled.
	metadataQueue.join()
	loggingger.info('Took {} seconds to parse complete source'.format(time.time() -ts))



if __name__ == '__main__':
   main()