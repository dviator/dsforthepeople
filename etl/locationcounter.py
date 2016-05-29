# for reading in configurations and paths
import logging
import configparser
import os

# for efficient searching
import re

# for importing csv files
import csv

# for multiprocessing
from functools import partial
from multiprocessing import Pool
import sys
import time

#setup
logging.basicConfig(filename='LocationCounter.log',level=logging.INFO,format='%(asctime)s %(levelname)s: %(message)s')

config = configparser.ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),"../","crawlers/","newscrawler.conf"))

data_root_dir = config.get('parsearticle','data_root_dir')

#Fetch list of locations.
def getCountries(): 
	countries = {}
	locations_fn = data_root_dir + "/locations/country_codes.csv"
	with open(locations_fn) as locationsfile:
		reader = csv.reader(locationsfile)
		for row in reader:
			countries[row[0]] = row[1]
	return countries

#Container (Dictionary of Dictionaries) to store information
def initialize_container():
	container = {}
	countries = getCountries()
	for place in countries:
		container[place] = {
			"code": countries[place],
			"count": {}
		}
	return container

# Function to process each file
# def process_file(container, articleid):
def process_file(articleid):
	''' Process one file: count occurrences of country names in this file '''
	container = initialize_container() #FIXME: need NOT to initialize container every time I process a file!!!
	with open(articleid, 'r') as inp:
		for line in inp:
			for place in container:
				if re.search(str(place), line, re.IGNORECASE):
					if articleid in container[place]["count"]:
						container[place]["count"][articleid] += 1
					else:
						container[place]["count"][articleid] = 0

# partial_process_file = partial(process_file, container=initialize_container())

# Function to process all files in parallel
# def process_files_parallel(arg, dirname, names):
def process_files_parallel(paths):
	''' process each file in via map() '''
	print ("inside process_files_parallel()")
	pool=Pool()
	results=pool.map(process_file, paths)
	# results=pool.map(partial_process_file, paths)
	# results=pool.map(process_file_partial, [os.path.join(dirname, name) for name in names])

#Convert to lowercase

#Determine which dataset to work with.

#Open a file.
# filename = data_root_dir + "/nytimes/fullText/" + "4978001668131110631"
# contents = []
# with open(filename) as article:
#     for line in article:
#         contents.append(line)

# Main
# if __name__ == '__main__':
data_dir = data_root_dir + "/nytimes/fullText/"
start=time.time()
paths=[]
for root, dirs, files in os.walk(data_dir, topdown=False):
    for name in files:
        paths.append(os.path.join(root, name))
    for name in dirs:
        paths.append(os.path.join(root, name))
process_files_parallel(paths)
# os.walk(data_dir, process_files_parallel, None)
print ("process_files_parallel() ",time.time()-start,sep=" ")

# print ("counts are: ", location_counts)
#join filename to entry in metadata table/database row
#Write csv row or database row containing location info and article info.
#Do the same with next file.




