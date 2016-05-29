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

class DownloadWorker(Thread):
	
	def __init__(self, queue):
		Thread.__init__(self)
		self.queue = queue

	def run(self):
		i = 0
		worker_start = time.time()
		while True:
			url, source, urlDate = self.queue.get()
			task_start = time.time()
			if i % 100 == 0:
				logging.info("{} articles parsed in {} seconds".format(i,time.time()-worker_start))
				logging.info("Calling parse with args: {}, {}, {}".format(url,source,urlDate))
			try:
				parsearticle.parse(url, source, urlDate)
				logging.info("Parsed {} article {} in {} seconds".format(source,url,time.time()-task_start))

			except newspaper.article.ArticleException as e:
				# print("I see an exception")
				logging.exception("Exception trying to parse article at url {}".format(url))
				pass
			
			self.queue.task_done()
			i+=1
			#Need to implement a function to stop these threads
class MetadataWriterWorker(Thread):
	pass

def main():
	ts = time.time()
	#Create queue to communicate with worker threads
	queue = Queue()
	#Create numThreads Worker Threads
	for x in range(numThreads):
		worker = DownloadWorker(queue)
		worker.daemon = True
		worker.start()
		# print("Started worker")
	#Call source specific crawler(s) to enqueue tasks
	logging.info("Begin queueing links")
	nytimes.crawl_nytimes_archive(queue)
	queue.join()
	logging.info('Took {} seconds to parse complete source'.format(time.time() -ts))



if __name__ == '__main__':
   main()