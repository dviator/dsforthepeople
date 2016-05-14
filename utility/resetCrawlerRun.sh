#!/bin/bash

#Set up write paths
#AWS Path
#data_root_dir = "/data"
#Dan's local path

text_dir="/home/maevyn/Documents/dsforthepeople/data/nytimes/fullText/"
metadata_dir="/home/maevyn/Documents/dsforthepeople/data/nytimes/metadata/*"
log_file="/home/maevyn/Documents/dsforthepeople/crawlers/NewsCrawler.log"
echo "Are you sure you want to delete all the crawler data?"
read answer
echo $answer
if [ "$answer" = "y" ]; then
	find $text_dir -delete
	rm $metadata_dir
	rm $log_file
	mkdir $text_dir
	echo "$text_dir and $metadata_dir and $log_file have been deleted"
else
	echo "No action taken"
fi
