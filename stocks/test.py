import stockretriever as stocks

# Get current stock information - returns most of 
# the information on a typical Yahoo! Finance stock page
info = stocks.get_current_info(["YHOO","AAPL","GOOG","MSFT"])
print info

# Get current stock news - returns the RSS feed for
# the given ticker in JSON format
news = stocks.get_news_feed('YHOO')

# Get historical prices - returns all historical
# open/low/high/close/volumn for thie given ticker
news = stocks.get_historical_info('YHOO')
