""" Tumblr API Example - Python CLI
"""
from datetime import timedelta 
import datetime
import oauth2
from bs4 import BeautifulSoup as bs 
import urlparse
import pytumblr
# import re

# def cleanhtml(raw_html):
#   cleanr = re.compile('<.*?>')
#   cleantext = re.sub(cleanr, '', raw_html)
#   return cleantext

REQUEST_TOKEN_URL = 'http://www.tumblr.com/oauth/request_token'
AUTHORIZATION_URL = 'http://www.tumblr.com/oauth/authorize'
ACCESS_TOKEN_URL = 'http://www.tumblr.com/oauth/access_token'
CONSUMER_KEY = 'V3d6XXDyA2Mhet8YGcnBEP6FwJ7EZEhb8xYD5t1IjDnW3ivDaO'
CONSUMER_SECRET = 'DkfPLb5pYNdRcdEjSpLz6iLzZR46VRArFB06rCEyLwGNBKnlYh'

# consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
# client = oauth2.Client(consumer)  

# resp, content = client.request(REQUEST_TOKEN_URL, "GET")

# request_token = dict(urlparse.parse_qsl(content))
# OAUTH_TOKEN = request_token['oauth_token']
# OAUTH_TOKEN_SECRET = request_token['oauth_token_secret']

# print "Request Token:"
# print "    - oauth_token        = %s" % OAUTH_TOKEN
# print "    - oauth_token_secret = %s" % OAUTH_TOKEN_SECRET

# client = pytumblr.TumblrRestClient(
#     CONSUMER_KEY,
#     CONSUMER_SECRET,
#     OAUTH_TOKEN,
#     OAUTH_TOKEN_SECRET
# )
client = pytumblr.TumblrRestClient('V3d6XXDyA2Mhet8YGcnBEP6FwJ7EZEhb8xYD5t1IjDnW3ivDaO')
# print "yo"
def search(keyword):
    for i in range(10):
        date = datetime.datetime.today()-timedelta(days=i)
        date = date.strftime("%s")
        print "searching for keyword " +keyword +  "date before " + date
        try:
            a = client.tagged(keyword,before=date)
        except:
            print "oops ran into an error"
            return 
        # print "yo"
        for ax in a:
            # print ax
            # idx = ax.index('body')
            try:
                raw = ax['trail'][0]['content']
                soup = bs(raw)
                text = soup.text
                # print raw
                # text = cleanhtml(raw)
                # print raw
                print "##############################"
            except:
                text = ax['summary']
                # print text
                print "???????????????????????????????? "
            user = ax['blog_name']
            location = -1
            hashtags =ax['tags']
            post_id = ax['id']
            RT = -1
            fav = -1
            source = 'tumblr'
            date_added = ax['date'][:10]
            in_reply_to = -1
            timestamp = ax['timestamp']
            obj = {}
            jsonx = {"_id": id, 'keyword': keyword, "username": user, "user_id": -1, "content": text, "timestamp": timestamp,
                         "location": location, "hashtags": hashtags, "Retweet": RT, "Fav": fav, 'source': source,
                         'date_added': date_added, 'in_reply_to': in_reply_to}    
            # try:
            #     coll.insert(jsonx)  # and insert
            #     count_ins += 1
            # except pymongo.errors.DuplicateKeyError:  # Except if it's already there
            #     count_dup += 1
            #     print("Duplicate Key")
            print (keyword + ":  \ntimestamp: " +'\n' + text )
            print "###########################" 
file = open('keywords.txt', 'r')
print "opened"
num_lines = 0
for line in file:
    if line.strip():
        num_lines += 1
print num_lines

file = open('keywords.txt', 'r')

for keyword in file:
    keyword = keyword.rstrip()
    print keyword
    if not keyword: continue
    # print(f"Keyword: {i}/{num_lines},    Current Keyword: {keyword}")
    query = keyword
    search(query)   

