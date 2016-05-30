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

#Set program to log to a file
crawlLogger = logging.basicConfig(filename=root_path+'/NewsCrawler.log',level=logging.INFO,format='%(asctime)s %(threadName)s %(levelname)s: %(message)s')
config = configparser.ConfigParser()
config.read(root_path+"/newscrawler.conf")
numThreads = config.getint('crawler','threads') 
newsSource = config.get('crawler','newsSource')

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
				logging.info("Calling parse with args: {}, {}, {}".format(url,source,urlDate))
			try:
				title, date, url, authors, newsSource, article_text_filename = parsearticle.parse(url, source, urlDate)
				# print("I got from parse {},{},{},{},{},{}".format(title,date,url,authors,newsSource,article_text_filename))
				self.metadataQueue.put((title, date, url, authors, newsSource, article_text_filename))
				# print(self.metadataQueue.qsize())

				logging.info("Parsed {} article {} in {} seconds".format(source,url,time.time()-task_start))

			except newspaper.article.ArticleException as e:
				# print("I see an exception")
				logging.exception("Exception trying to parse article at url {}".format(url))
				pass
			
			self.queue.task_done()
			i+=1
			#Need to implement a function to stop these threads

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
	logging.info('Took {} seconds to parse complete source'.format(time.time() -ts))



if __name__ == '__main__':
   main()