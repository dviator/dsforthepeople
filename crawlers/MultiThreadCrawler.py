from queue import Queue
from threading import Thread
import time
import logging

from parseArticle import parse
from nytimes import crawl_nytimes_archive



class DownloadWorker(Thread):
	def __init__(self, queue):
		Thread.__init__(self)
		self.queue = queue

	def run(self):
		i = 0
		worker_start = time.time()
		while True:
			link, source, urlDate = self.queue.get()
			task_start = time.time()
			if i % 100 == 0:
				logging.info("{} articles parsed in {} seconds".format(i,time.time()-worker_start))
			logging.info("Calling parse with args: {}, {}, {}".format(link,source,urlDate))
			parse(link, source, urlDate)
			logging.info("Parsed {} article {} in {} seconds".format(source,link,time.time()-task_start))
			self.queue.task_done()
			i+=1

def main():
	#Set program to log to a file
	logging.basicConfig(filename='NewsCrawler.log',level=logging.INFO,format='%(asctime)s %(threadName)s %(message)s')
	ts = time.time()
	#Create queue to communicate with worker threads
	queue = Queue()
	#Create 10 Worker Threads
	for x in range(10):
		worker = DownloadWorker(queue)
		worker.daemon = True
		worker.start()
	#Call source specific crawler(s) to enqueue tasks
	logging.info("Begin queueing links")
	crawl_nytimes_archive(queue)
	queue.join()
	print('Took {} seconds to parse complete source'.format(time.time() -ts))



if __name__ == '__main__':
   main()