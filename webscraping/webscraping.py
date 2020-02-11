import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import re

"""
A simple python script for scraping reddit data fron pushshift.io.
"""

# Set the URL you want to webscrape from
url = 'https://files.pushshift.io/reddit/comments/'

# Connect to the URL
response = requests.get(url)

# Parse HTML and save to BeautifulSoup object¶
soup = BeautifulSoup(response.text, "html.parser")

# To download the whole data set, let's do a for loop through all a tags
line_count = 1 #variable to track what line you are on
for one_a_tag in soup.findAll('a'):  #'a' tags are for links
    for obj in one_a_tag:
        if "JSON" in obj:
            link = one_a_tag['href']
            print(link)
            try:
                download_url = url + link
                urllib.request.urlretrieve(download_url,'./reddit-comments/'+link[link.find('/reddit_comments')+1:])
            except:
                print(link + "not found ...")
                continue
