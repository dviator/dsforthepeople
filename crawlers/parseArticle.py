import newspaper as np
import csv
import random
import time
import logging

logging.basicConfig(filename='PerformanceStats.log',level=logging.INFO,format='%(asctime)s %(threadName)s %(message)s')

#Set up write paths
#data_root_dir = "/data"
data_root_dir = "/home/maevyn/Documents/dsforthepeople/data"



def parse(url, newsSource):
	complete_parse_start = time.time()
	#Collect data about each article
	article = np.Article(url)
	download_start = time.time()
	article.download()
	logging.info("Download completed in {} seconds".format(time.time()- download_start))
	parse_start = time.time()
	article.parse()
	logging.info("Parse completed in {} seconds".format(time.time()- parse_start))

	#Store metadata to variables
	title = article.title
	authors = article.authors
	text = article.text
	date = article.publish_date
	# meta_keywords = article.meta_keywords
	# meta_description = article.meta_description

	#Build up unique filename for each articles full text snippet
	#Append a random value to title in case there are duplicate titles
	#Title may includes /'s, which breaks things. Need to escape these or do something else'
	random_num = random.getrandbits(64)
	filename = str(random_num)

	csv_start = time.time()
	metadata_path = data_root_dir + "/" + newsSource + "/metadata/" + newsSource + "Articles.txt"
	logging.info("Metadata path is {}".format(metadata_path))
	#Write metadata and reference to full text file name to csv separated by special character
	with open(metadata_path, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='\u001c',quoting=csv.QUOTE_ALL)
		articleWriter.writerow([title,date,authors,newsSource,filename])
	logging.info("CSV write completed in {} seconds".format(time.time()- csv_start))

	# print(title)
	# print(authors)
	# print(date)
	# print(tags)
	# print(meta_keywords)
	# print(meta_description)
	print("Row Writer complete")

	full_text_path = data_root_dir + "/" + newsSource + "/fullText/" + filename
	logging.info("Full text path is {}".format(full_text_path))
	text_start = time.time()
	fullTextFile = open(full_text_path,'w')
	fullTextFile.write(text)
	logging.info("Full text write completed in {} seconds".format(time.time()- text_start))

	logging.info("Complete parse function completed in {} seconds".format(time.time()- parse_start))


#Test article to parse when module is called directly from command line
#url = "http://www.nytimes.com/2016/03/26/world/middleeast/abd-al-rahman-mustafa-al-qaduli-isis-reported-killed-in-syria.html?hp&action=click&pgtype=Homepage&clickSource=story-heading&module=first-column-region&region=top-news&WT.nav=top-news"
#parse(url,"test")