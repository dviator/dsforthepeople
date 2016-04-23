#!/bin/bash

#Set up write paths
#AWS Path
#data_root_dir = "/data"
#Dan's local path

#text_dir="/home/maevyn/Documents/dsforthepeople/data/fulltext/*"
#metadata_dir="/home/maevyn/Documents/dsforthepeople/data/metadata/*"

echo "Are you sure you want to delete all the crawler data?"
read answer

if ($answer == "y"); then
	#rm $text_dir
	#rm $metadata_dir
	#echo "$text_dir and $metadata_dir have been deleted"
else
	echo "No action taken"
fi