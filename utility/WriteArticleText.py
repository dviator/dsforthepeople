from newspaper import Article

url = "http://www.nytimes.com/2016/03/27/magazine/bill-waltons-long-strange-tale-of-nba-survival.html?hp&action=click&pgtype=Homepage&clickSource=story-heading&module=second-column-region&region=top-news&WT.nav=top-news&_r=0"

article = Article(url=url)

article.download()
article.parse()

print(article.text)
