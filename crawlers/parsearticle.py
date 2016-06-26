import newspaper
import csv
import random
import time
import logging
import os.path
import datetime
import configparser
import sys
from retrying import retry
import hashlib
import requests

config = configparser.ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),"newscrawler.conf"))
data_root_dir = config.get('parsearticle','data_root_dir')

#Configure another logger for the recording of runtime statistics for the parsing component
stats_logger = logging.getLogger('stats_logger')


def parse(url, newsSource, urlDate, metadataQueue):
	logging.info("Calling parse with args: {}, {}, {}".format(url,newsSource,urlDate))
	parse_start = time.time()
	#Collect data about each article
	article, download_duration = getArticle(url)
	logging.info("{} seconds to download article: {}".format(time.time()-parse_start,url))
	#Store metadata to variables
	title = article.title
	authors = article.authors
	text = article.text
	date = article.publish_date

	#If Newspaper can't get the date from the article use the one from the search URL
	if date is None:
		date = urlDate
	#Format date to be the same whether it's from url or Newspaper
	date = date.strftime('%m-%d-%Y')

	article_text_filename, write_text_duration, article_size = writeFullTextFile(newsSource,title,date,text)

	stats_logger.info("{},{},{},{}".format(url,article_size,download_duration,write_text_duration))
	#Send metadata about the article back to main thread to be written to central file
	#return title, date, url, authors, newsSource, article_text_filename
	
	#Instead, put this info directly on queue, to avoid passing back to main program
	metadataQueue.put((title, date, url, authors, newsSource, article_text_filename))
	logging.info("{} seconds to complete parse of article: {}".format(time.time()-parse_start,url))
	return

def writeFullTextFile(newsSource,title,date,text):
	text_start = time.time()
	#Compute a hash value for filename based on title,newsSource,date to store deterministic uniquely keyed file
	article_id = title + newsSource + str(date)
	article_id = article_id.encode('utf-16')
	hasher = hashlib.md5()
	hasher.update(article_id)
	article_text_filename = hasher.hexdigest()

	#Determine directory and filenaming, create fullText directory for source if it does not exist
	full_text_path = data_root_dir + "/" + newsSource + "/fullText/"
	full_text_file = os.path.join(full_text_path,article_text_filename)
	logging.debug("Full text path is {}".format(full_text_path))
	
	if not os.path.exists(full_text_path):
		os.makedirs(full_text_path)

	#Open file with mode 'x', exclusive creation, will raise error if file already exists.
	with open(full_text_file, 'x') as fullTextFile:
		fullTextFile.write(text)
	
	write_text_duration = time.time() - text_start
	article_size = os.path.getsize(full_text_file)

	logging.debug("Full text write to {} completed in {} seconds".format(full_text_path,time.time()- text_start))

	return article_text_filename, write_text_duration, article_size

def retry_if_request_error(exception):
	if isinstance(exception,newspaper.article.ArticleException):
		logging.warning("Retrying article")
		#Going to implement logic to check for a 410 response from requests indicating this is bad html. Otherwise assume network issue.
		return True
	else:
		return False

@retry(retry_on_exception=retry_if_request_error,wait_exponential_multiplier=250,wait_exponential_max=30000,stop_max_delay=600000)
def getArticle(url):
	logging.info("Attempting getArticle on url {}".format(url))
	article = newspaper.Article(url)
	download_start = time.time()
	article.download()
	#Check for html now that article is downloaded. If not, raise an error?
	parse_start = time.time()
	#Following commented code is not working skeleton code for disambiguating ArticleExceptions that should be retried and those that shouldn't.
	#Want to only invoke extra get if parse fails initially, and then decide on retry behavior by either raising a standard ArticleException which causes a retry
	#or raising a unique exception that will be raised upwards causing the article to be skipped over and logged.
	# try:
	article.parse()
	# except(newspaper.article.ArticleException):
	# 	url_status = requests.get(url).status_code
	# 	if url_status == 401:
	# 		print("url status 401")
	# 		raise HTMLNotFoundError("page at url {} could not be found".format(url))
	# 	else:
	# 		raise newspaper.article.ArticleException("URL status besides 401 on bad article.parse")

	logging.info("Downloading article at {} completed in {} seconds".format(url, time.time()- download_start))	
	# print(article)
	# print(dir(article))
	logging.debug("Parse completed in {} seconds".format(time.time()- parse_start))
	download_duration = time.time() - download_start
	return article, download_duration

def writeMetadataHeader(newsSource):
	metadata_dir = data_root_dir + "/" + newsSource + "/metadata/" 
	metadata_file = metadata_dir + newsSource + "Articles.txt"
	
	#If data directory doesn't exist write it. 
	if not os.path.exists(metadata_dir):
		os.makedirs(metadata_dir)

	#If file doesn't exist write column names in first row
	if os.path.isfile(metadata_file) is False:
		with open(metadata_file, 'a') as csvfile:
			articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
			articleWriter.writerow(["Title","Date","URL","Authors","Source","fullTextID"])

	return

def writeMetadataRow(title, date, url, authors, newsSource, article_text_filename):
	#Logic for writing a metadata row, needs to be moved to a single threaded function
	csv_start = time.time()
	metadata_path = data_root_dir + "/" + newsSource + "/metadata/" + newsSource + "Articles.txt"
	logging.debug("Metadata path is {}".format(metadata_path))

	logging.info("Writing metadata row for article at {} to {}".format(url,metadata_path))
	#Write metadata and reference to full text file name to csv separated by ~ character
	with open(metadata_path, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow([title,date,url,authors,newsSource,article_text_filename])
	logging.debug("CSV write completed to {} in {} seconds".format(metadata_path,time.time()- csv_start))
	
	return

	#Write plaintext of article into file named with surrogate key reference to metadata entry in metadata file

class HTMLNotFoundError(Exception):
	def __init__(self,arg):
		self.args = arg