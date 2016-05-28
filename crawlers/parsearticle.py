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

config = configparser.ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),"newscrawler.conf"))
data_root_dir = config.get('parsearticle','data_root_dir')


def parse(url, newsSource, urldate):
	complete_parse_start = time.time()
	#Collect data about each article
	article = getArticle(url)

	#Store metadata to variables
	title = article.title
	authors = article.authors
	text = article.text
	date = article.publish_date

	#If Newspaper can't get the date from the article use the one from the search URL
	if date is None:
		date = urldate
	#Format date to be the same whether it's from url or Newspaper
	date = date.strftime('%m-%d-%Y')

	article_text_filename = writeFullTextFile(newsSource,text)
	logging.info("Parse function of url {} completed in {} seconds".format(url,time.time()- complete_parse_start))

	#Send metadata about the article back to main thread to be written to central file
	return title, date, url, authors, newsSource, article_text_filename
	
def writeMetadataHeader(newsSource):
	metadata_path = data_root_dir + "/" + newsSource + "/metadata/" + newsSource + "Articles.txt"
	#If file doesn't exist write column names in first row
	# if not os.path.isfile(metadata_path):
	with open(metadata_path, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow(["Title","Date","URL","Authors","Source","fullTextID"])

	return

def writeMetadataRow(title, date, url, authors, newsSource, article_text_filename):
	#Logic for writing a metadata row, needs to be moved to a single threaded function
	csv_start = time.time()
	metadata_path = data_root_dir + "/" + newsSource + "/metadata/" + newsSource + "Articles.txt"
	logging.debug("Metadata path is {}".format(metadata_path))

	#Write metadata and reference to full text file name to csv separated by ~ character
	with open(metadata_path, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow([title,date,url,authors,newsSource,article_text_filename])
	logging.debug("CSV write completed to {} in {} seconds".format(metadata_path,time.time()- csv_start))
	
	return

	#Write plaintext of article into file named with surrogate key reference to metadata entry in metadata file

def retry_if_request_error(exception):
	if isinstance(exception,newspaper.article.ArticleException):
		return True
	else:
		return False

@retry(retry_on_exception=retry_if_request_error,wait_exponential_multiplier=250,wait_exponential_max=30000,stop_max_delay=600000,wrap_exception=True)
def getArticle(url):
	article = newspaper.Article(url)
	download_start = time.time()
	article.download()
	parse_start = time.time()
	article.parse()
	logging.info("Downloading article at {} completed in {} seconds".format(url, time.time()- download_start))	
	# print(article)
	# print(dir(article))
	logging.debug("Parse completed in {} seconds".format(time.time()- parse_start))
	return article

def writeFullTextFile(newsSource,text):
	#Use a randomized surrogate key to identify each fullText file in the metadata table
	#Write this key as a filename for reference later
	random_num = random.getrandbits(64)
	article_text_filename = str(random_num)

	full_text_path =data_root_dir + "/" + newsSource + "/fullText/"
	logging.debug("Full text path is {}".format(full_text_path))
	text_start = time.time()
	if not os.path.exists(full_text_path):
		os.makedirs(full_text_path)

	with open(os.path.join(full_text_path,article_text_filename), 'w') as fullTextFile:
		fullTextFile.write(text)

	logging.debug("Full text write to {} completed in {} seconds".format(full_text_path,time.time()- text_start))

	return article_text_filename