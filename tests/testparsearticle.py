import unittest
from unittest.mock import MagicMock, patch
from crawlers import parsearticle
import datetime
import shutil
import os
import logging
import newspaper
import random
from queue import Queue
import csv
import timeout_decorator

class TestParseArticle(unittest.TestCase):

	url = 'http://www.nytimes.com/2007/01/01/sports/ncaafootball/01cnd-outback.html?_r=0'
	newsSource = "test"
	urldate = datetime.date(2007,1,1)
	test_data_dir = parsearticle.data_root_dir + "/" + newsSource
	fullText_dir = test_data_dir + "/fullText"
	metadata_dir = test_data_dir + "/metadata/"
	test_resources = os.path.join(os.path.abspath(os.path.dirname(__file__)),"resources/")
	with open(test_resources+"sampleFullText.txt") as f:
		sample_fullText = f.read()
	
	sample_title = "With Paterno Coaching From Above, Penn St. Takes Outback Bowl"
	sample_date = "01-01-2007"
	sample_authors = ['Viv Bernstein'] 
	maxDiff = None


	#Create the directories to store results for "test" newsSource
	def setUp(self):
		logging.disable(logging.CRITICAL)

		if not os.path.exists(self.fullText_dir):
			os.makedirs(self.fullText_dir)
		if not os.path.exists(self.metadata_dir):
			os.makedirs(self.metadata_dir)	

	#Delete previously created directories to ensure clean environment.
	def tearDown(self):
		shutil.rmtree(self.test_data_dir)
		pass

	def test_writes_fulltext_file(self):
		metadataQueue = Queue()
		#Run the parse on an article.
		parsearticle.parse(self.url,self.newsSource,self.urldate,metadataQueue)
		#Fetch the file that the run should've written and store it's string contents in testing_fullText
		testing_fullTextFileNames = os.listdir(self.fullText_dir)
		testing_fullTextFileName = testing_fullTextFileNames.pop()
		with open(os.path.join(self.fullText_dir,testing_fullTextFileName)) as f:
			testing_fullText = f.read()

		#Confirm file is equivalent to manually validated output.	
		self.assertEqual(self.sample_fullText,testing_fullText)

	#Test that the parse function returns the data necessary for another thread to write it to the metadata csv
	def test_queues_metadata_row(self):
		metadataQueue = Queue()

		parsearticle.parse(self.url,self.newsSource,self.urldate, metadataQueue)
		title, date, url, authors, newsSource, filename = metadataQueue.get()
		self.assertEqual(self.sample_title,title)
		self.assertEqual(self.sample_date,date)
		self.assertEqual(self.url,url)
		self.assertEqual(self.sample_authors,authors)
		self.assertEqual(self.newsSource,newsSource)
		#Get name of fulltext file written to ensure it matches the name that'll be stored in the metadata table.
		testing_fullTextFileNames = os.listdir(self.fullText_dir)
		self.assertEqual(testing_fullTextFileNames[0],filename)

	#Some of this testing needs to move to testcrawler
	# sample_metadata_row = "TODO"
	# sample_header_row = "TODO"
	# metadataQueue = Queue()
	# line = metadataFile.nextline()
	# self.assertEqual(sample_header_row,line)
	# metadataQueue.put(())

	def test_write_metadata_header(self):
		sample_header = '''"Title"~"Date"~"URL"~"Authors"~"Source"~"fullTextID"'''	
		sample_header_listing = ['Title','Date','URL','Authors','Source','fullTextID']

		parsearticle.writeMetadataHeader(self.newsSource)

		testing_metadataFileNames = os.listdir(self.metadata_dir)
		with open(os.path.join(self.metadata_dir,testing_metadataFileNames[0]),'r') as metadataFile:
			csvreader = csv.reader(metadataFile, delimiter = '~')
			
			for row in csvreader:
				header = row
				# tested_title = row[0]
		self.assertEqual(sample_header_listing,header)
		
	def test_write_metadata_row(self):

		sample_metadata_row = '''"With Paterno Coaching From Above, Penn St. Takes Outback Bowl"~"01-01-2007"~"http://www.nytimes.com/2007/01/01/sports/ncaafootball/01cnd-outback.html?_r=0"~"['Viv Bernstein']"~"test"~"+"'''
		random_num = random.getrandbits(64)
		filename = str(random_num)

		parsearticle.writeMetadataRow(self.sample_title, self.sample_date, self.url, self.sample_authors, self.newsSource, filename)

		testing_metadataFileNames = os.listdir(self.metadata_dir)
		
		with open(os.path.join(self.metadata_dir,testing_metadataFileNames[0]),'r') as metadataFile:
			csvreader = csv.reader(metadataFile, delimiter = '~')
			for row in csvreader:
				tested_title = row[0]
				tested_date = row[1]
				tested_url = row[2]
				tested_authors = row[3]
				tested_newsSource = row[4]
				tested_filename = row[5]

		self.assertEqual(self.sample_title,tested_title)
		self.assertEqual(self.sample_date,tested_date)
		self.assertEqual(self.url,tested_url)
		self.assertEqual(str(self.sample_authors),tested_authors)
		self.assertEqual(self.newsSource,tested_newsSource)
		self.assertEqual(filename,tested_filename)

	@patch('newspaper.Article.parse')
	def test_getArticle_backoff(self,mock_parse):
		mock_parse.side_effect = [newspaper.article.ArticleException("Article Exception"),mock_parse.DEFAULT]
		article = parsearticle.getArticle(self.url)

		self.assertIsNotNone(article)

	#Test for exception disambiguation in parsearticle
	# #Use timeout decorator as a workaround because failing test will cause 10 minutes of retrying.
	# @timeout_decorator.timeout(5)
	# def test_getArticle_backoff_does_not_retry_410_not_found_bad_html(self):
	# 	good_url = self.url
	# 	bad_url = "http://www.nytimes.com/aponline/2014/10/08/world/middleeast/ap-cn-canada-terrorism-threats.html"

	# 	with self.assertRaises(newspaper.article.ArticleException):
	# 		bad_article = parsearticle.getArticle(bad_url)



	def test_duplicate_article_fullText_write_caught(self):
		parsearticle.writeFullTextFile(self.newsSource,self.sample_title,self.sample_date,"test_text")
		with self.assertRaises(FileExistsError):
			parsearticle.writeFullTextFile(self.newsSource,self.sample_title,self.sample_date,"test_text")