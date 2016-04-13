import newspaper as np
import csv
import random
import time
import logging
import os.path
import datetime

logging.basicConfig(filename='NewsCrawler.log',level=logging.INFO,format='%(asctime)s %(threadName)s %(message)s')

#Set up write paths
#AWS Path
#data_root_dir = "/data"
#Dan's local path
data_root_dir = "/home/maevyn/Documents/dsforthepeople/data"



def parse(url, newsSource, urldate):
	complete_parse_start = time.time()
	#Collect data about each article
	article = np.Article(url)
	download_start = time.time()
	article.download()
	logging.info("Downloading article at {} completed in {} seconds".format(url, time.time()- download_start))
	parse_start = time.time()
	article.parse()
	logging.debug("Parse completed in {} seconds".format(time.time()- parse_start))

	#Store metadata to variables
	title = article.title
	authors = article.authors
	text = article.text
	date = article.publish_date
	
	# meta_keywords = article.meta_keywords
	# meta_description = article.meta_description

	#If Newspaper can't get the date from the article use the one from the search URL
	if date is None:
		print("Newspaper gathered date is of type: {}".format(type(date)))
		date = urldate
		print("URL gathered date is of type: {}".format(type(date)))
	#Format date to be the same whether it's from url or Newspaper
	date = date.strftime('%m-%d-%Y')
	#Build up unique filename for each articles full text snippet
	#Append a random value to title in case there are duplicate titles
	#Title may includes /'s, which breaks things. Need to escape these or do something else'
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

	# print(title)
	# print(authors)
	# print(date)
	# print(tags)
	# print(meta_keywords)
	# print(meta_description)
	#Debug print
	#print("Row Writer complete")

	full_text_path = data_root_dir + "/" + newsSource + "/fullText/" + filename
	logging.debug("Full text path is {}".format(full_text_path))
	text_start = time.time()
	fullTextFile = open(full_text_path,'w')
	fullTextFile.write(text)
	logging.debug("Full text write to {} completed in {} seconds".format(full_text_path,time.time()- text_start))

	logging.info("Parse function of url {} completed in {} seconds".format(url,time.time()- parse_start))


#Test article to parse when module is called directly from command line
#Turn off before running multithreader or will break
#url = "http://www.nytimes.com/2016/03/26/world/middleeast/abd-al-rahman-mustafa-al-qaduli-isis-reported-killed-in-syria.html?hp&action=click&pgtype=Homepage&clickSource=story-heading&module=first-column-region&region=top-news&WT.nav=top-news"
#parse(url,"test","2007-01-02")