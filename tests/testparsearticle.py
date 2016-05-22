import unittest
from unittest.mock import MagicMock, patch
from crawlers import parsearticle
import datetime
import shutil
import os
import logging
import newspaper

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
		#Run the parse on an article.
		parsearticle.parse(self.url,self.newsSource,self.urldate)
		#Fetch the file that the run should've written and store it's string contents in testing_fullText
		testing_fullTextFileNames = os.listdir(self.fullText_dir)
		testing_fullTextFileName = testing_fullTextFileNames.pop()
		with open(os.path.join(self.fullText_dir,testing_fullTextFileName)) as f:
			testing_fullText = f.read()

		#Confirm file is equivalent to manually validated output.	
		self.assertEqual(self.sample_fullText,testing_fullText)

	#Test that the parse function returns the data necessary for another thread to write it to the metadata csv
	def test_returns_metadata_row(self):
		title, date, url, authors, newsSource, filename = parsearticle.parse(self.url,self.newsSource,self.urldate)
		self.assertEqual(self.sample_title,title)
		self.assertEqual(self.sample_date,date)
		self.assertEqual(self.url,url)
		self.assertEqual(self.sample_authors,authors)
		self.assertEqual(self.newsSource,newsSource)
		#Get name of fulltext file written to ensure it matches the name that'll be stored in the metadata table.
		testing_fullTextFileNames = os.listdir(self.fullText_dir)
		self.assertEqual(testing_fullTextFileNames[0],filename)

	@patch('newspaper.Article.parse')
	def test_getArticle_backoff(self,mock_parse):
		mock_parse.side_effect = [newspaper.article.ArticleException("Article Exception"),mock_parse.DEFAULT]
		article = parsearticle.getArticle(self.url)
		self.assertIsNotNone(article)
