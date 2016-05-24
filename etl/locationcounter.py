import logging
import configparser
import os
import re

logging.basicConfig(filename='LocationCounter.log',level=logging.INFO,format='%(asctime)s %(levelname)s: %(message)s')

config = configparser.ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),"../","crawlers/","newscrawler.conf"))

data_root_dir = config.get('parsearticle','data_root_dir')
#Fetch list of locations.
#Placeholder until I get connected up to the database in AWS.
locations = ['kenya','congo','new york','china','united states','bangladesh','texas','timbuktu', 'seattle']
#Convert to lowercase

#Determine which dataset to work with.

#Open a file.
filename = data_root_dir + "/nytimes/fullText/" + "4978001668131110631"
contents = []
with open(filename) as article:
    for line in article:
        contents.append(line)

#count occurrences of strings in the file and capture in dict? or other datatype
location_counts = {}
for place in locations:
	location_counts[place] = 0;

for element in contents:
	for place in locations:
		if re.search(place, element, re.IGNORECASE):
			print (element)
			print (place)
			location_counts[place] += 1

print ("counts are: ", location_counts)
#join filename to entry in metadata table/database row
#Write csv row or database row containing location info and article info.
#Do the same with next file.




