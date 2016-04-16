import newspaper as np
import csv
import random
import time
import logging
import os.path
import datetime
import configparser

logging.basicConfig(filename='NewsCrawler.log',level=logging.INFO,format='%(asctime)s %(threadName)s %(message)s')
config = configparser.ConfigParser()
config.read("newscrawler.conf")
data_root_dir = config.get('parsearticle','data_root_dir')

def parse(url, newsSource, urldate):
	complete_parse_start = time.time()
	#Collect data about each article
	article = np.Article(url)
	download_start = time.time()
	try:
		article.download()
	except:
		print("For article at url {}, exception block entered".format(url))
		e = sys.exc_info()[0]
		logging.info("parsearticle Error: {}".format(e))
		return
	logging.info("Downloading article at {} completed in {} seconds".format(url, time.time()- download_start))	
	
	parse_start = time.time()
	article.parse()
	logging.debug("Parse completed in {} seconds".format(time.time()- parse_start))

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

	#Build up unique filename for each article's full text snippet
	#Append a random value to title in case there are duplicate titles
	random_num = random.getrandbits(64)
	filename = str(random_num)

	csv_start = time.time()
	metadata_path = data_root_dir + "/" + newsSource + "/metadata/" + newsSource + "Articles.txt"
	logging.debug("Metadata path is {}".format(metadata_path))

	#If file doesn't exist write column names in first row
	if not os.path.isfile(metadata_path):
		with open(metadata_path, 'a') as csvfile:
			articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
			articleWriter.writerow(["Title","Date","URL","Authors","Source","fullTextID"])
	#Write metadata and reference to full text file name to csv separated by special character
	with open(metadata_path, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow([title,date,url,authors,newsSource,filename])
	logging.debug("CSV write completed to {} in {} seconds".format(metadata_path,time.time()- csv_start))

	#Write plaintext of article into file named with surrogate key reference to metadata entry in metadata file
	full_text_path = data_root_dir + "/" + newsSource + "/fullText/" + filename
	logging.debug("Full text path is {}".format(full_text_path))
	text_start = time.time()
	#Want to make this use with for safety. This could be breaking things with too many open filehandles
	with open(full_text_path, 'w') as fullTextFile:
		fullTextFile.write(text)

	#fullTextFile = open(full_text_path,'w')
	#fullTextFile.write(text)
	logging.debug("Full text write to {} completed in {} seconds".format(full_text_path,time.time()- text_start))
	logging.info("Parse function of url {} completed in {} seconds".format(url,time.time()- parse_start))


#Test article to parse when module is called directly from command line
#Turn off before running multithreader or will break
#url = "http://www.nytimes.com/2016/03/26/world/middleeast/abd-al-rahman-mustafa-al-qaduli-isis-reported-killed-in-syria.html?hp&action=click&pgtype=Homepage&clickSource=story-heading&module=first-column-region&region=top-news&WT.nav=top-news"
#parse(url,"test","2007-01-02")