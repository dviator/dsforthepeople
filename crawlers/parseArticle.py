import newspaper as np
import csv
def parse(url):
	article = np.Article(url)
	article.download()
	article.parse()

	title = article.title
	authors = article.authors
	text = article.text
	date = article.publish_date

	with open('/home/maevyn/Documents/dsforthepeople/data/nytimes', 'w') as csvfile:
		articleWriter = csv.writer(csvfile, delimiter='\t',quoting=csv.QUOTE_ALL)
		articleWriter.writerow(title,date,authors,text)
	# print(title)
	# print(authors)
	# print(text)
	print("Row Writer complete")
url = "http://www.nytimes.com/2016/03/26/world/middleeast/abd-al-rahman-mustafa-al-qaduli-isis-reported-killed-in-syria.html?hp&action=click&pgtype=Homepage&clickSource=story-heading&module=first-column-region&region=top-news&WT.nav=top-news"
parse(url)