create table kill_floor.nytimeslocations (
Title varchar(250),
Date Date,
URL varchar(250),
Source varchar(10),
fullTextId varchar(32),
Country varchar(3),
Count smallint)

#Command line to load table from newscrawler instance
#I omitted Authors column because Arrays were tricky and we didn't need that data
#Ran in <30 seconds on newscrawler instance
\copy kill_floor.nytimeslocations from '/home/danielveenstra/articlecountsnoauthor.csv' with csv delimiter '~' quote '"' header