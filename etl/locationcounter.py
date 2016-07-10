#########################################################################################################################
# IMPORT STATEMENTS
#########################################################################################################################

# for reading in configurations and paths
import logging
import configparser
import os

# for efficient searching
import re

# for importing/writing csv files
import csv

# for multiprocessing
from functools import partial
from multiprocessing import Pool
import sys
import time

# Global Variables
global COUNTRIES
global OUTFILE

#########################################################################################################################
# CONFIGURATIONS
#########################################################################################################################

# Setup for Logging, Global Variable Definitions, 
logging.basicConfig(filename='LocationCounter.log',level=logging.INFO,format='%(asctime)s %(levelname)s: %(message)s')
config = configparser.ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),"../","crawlers/","newscrawler.conf"))
data_root_dir = config.get('parsearticle','data_root_dir')
OUTFILE = data_root_dir + "/etl_output/article_counts.csv"
METADATAFILE = data_root_dir + "/nytimes/metadata/nytimesArticles.txt"

#########################################################################################################################
# Function Definitions
#########################################################################################################################

def getCountries(): 
	''' Fetch list of locations with corresponding ISO country code '''

	countries = {}
	locations_fn = os.path.abspath(os.path.dirname(__file__))+"/../data/locations/country_codes.csv"
	with open(locations_fn) as locationsfile:
		reader = csv.reader(locationsfile)
		for row in reader:
			countries[row[0]] = row[1]

	return countries

#Functions for output file formatting
def writeMetadataHeader(filename):
	''' Write Header to the ETL Output File '''
	with open(filename, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow(["Title","Date","URL","Authors","Source","fullTextID","Country","Count"])
	
	return

def writeMetadataRow(articleid, country, count):
	''' Logic for writing a metadata row, needs to be moved to a single threaded function '''
	
	csv_start = time.time()
	logging.debug("Metadata path is {}".format(OUTFILE))

	#FIXME: temporarily fix these input variables to zero since no metadata is yet available
	title = ""
	date = ""
	url = ""
	authors = ""
	newsSource = ""

	#Write metadata and reference to full text file name to csv separated by ~ character
	with open(OUTFILE, 'a') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL)
		articleWriter.writerow([title,date,url,authors,newsSource,articleid,country,count])
	logging.debug("CSV write completed to {} in {} seconds".format(OUTFILE,time.time()- csv_start))
	
	return

def readMetadata():
	''' return a dict containing meta-data from crawler, to be put into output csv file '''
	metadict = {}
	with open(METADATAFILE, 'r') as metafile:
		metareader = csv.DictReader(metafile, delimiter="~", quoting=csv.QUOTE_ALL)
		for row in metareader:
			metadict.update({

				row['fullTextID']:

				{'Title': row['Title'], 
				'Date': row['Date'], 
				'URL': row['URL'], 
				'Authors': row['Authors'], 
				'Source': row['Source'],
				'fullTextID': row['fullTextID'],
				'Country': '',
				'Count': ''}
			})

	return metadict

def writeMetadataFile():
	''' concatenate counts of country names in each article with its corresponding metadata for final output '''
	tempfilename = OUTFILE + ".tmp"
	metadict = readMetadata() 
	with open(tempfilename, 'a') as tempfile:
		metawriter = csv.writer(tempfile, delimiter="~", quoting=csv.QUOTE_ALL)
		with open(OUTFILE, 'r') as countfile:
			countreader = csv.DictReader(countfile, delimiter="~", quoting=csv.QUOTE_ALL)
			for row in countreader:
				key = row['fullTextID']
				if key in metadict:
					metadict[key]['Country'] = row['Country']
					metadict[key]['Count']   = row['Count']
					metawriter.writerow(
						[metadict[key]['Title'],
						metadict[key]['Date'],
						metadict[key]['URL'],
						metadict[key]['Authors'],
						metadict[key]['Source'],
						metadict[key]['fullTextID'],
						metadict[key]['Country'],
						metadict[key]['Count']]
						)

	return



def process_file(article_path):
	''' Process one file: count occurrences of country names in this file '''

	articleid = os.path.basename(os.path.normpath(article_path))
	with open(article_path) as inp:
		lines = inp.readlines()
	# For each country, search every line of the input file for it's occurrence
	for place in COUNTRIES:
		flag = False
		count = 0
		for line in lines:
			if re.search(place, line, re.IGNORECASE):
				flag = True
				count += 1
		# If this country's name was found at all, print it's count to the output file
		if flag:
			writeMetadataRow(articleid, COUNTRIES[place], str(count))

	return

# Function to process all files in parallel
def process_files_parallel(paths):
	''' process each file in via map() '''

	pool=Pool()
	results=pool.map(process_file, paths)

	return

#########################################################################################################################
# Main
#########################################################################################################################

if __name__ == '__main__':

	# Define some needed variables
	data_dir = data_root_dir + "/nytimes/fullText/"
	start=time.time()
	paths=[]
	COUNTRIES = getCountries()
	
	#Create locations directory if it doesn't exist
	if not os.path.exists(data_root_dir+"/etl_output/"):
		os.makedirs(data_root_dir+"/etl_output/")

	# Delete the output file if it currently exists
	if os.path.isfile(OUTFILE):
		os.remove(OUTFILE)

	# Create the Metadata output file
	writeMetadataHeader(OUTFILE)
	writeMetadataHeader(OUTFILE + ".tmp")

	# Get a list of paths to all articles 
	for root, dirs, files in os.walk(data_dir, topdown=False):
	    for name in files:
	        paths.append(os.path.join(root, name))
	    for name in dirs:
	        paths.append(os.path.join(root, name))
	
	# Send path list to core location-counting function
	process_files_parallel(paths)

	# Concatenate Metadata
	writeMetadataFile()

	# TO-DO
	# 1.  Determine which dataset to work with.
	# 2.  Input original metadata to currently empty CSV output columns
	# 3.  Refine search not just to countries, but to localities within those countries as well

	print ("process_files_parallel() ",time.time()-start,sep=" ")




