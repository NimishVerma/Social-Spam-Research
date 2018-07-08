import tweepy
import re
import pymongo
from datetime import datetime
from datetime import timedelta
import time

f = 0

file = open('../keywords.txt', 'r')

num_lines = 0
for line in file:
    if line.strip():
        num_lines += 1
num_keys = 12  # kept a little lower to compensate for the twitter limit risk factor
count_ins = count_dup = 0
max_items = int(num_keys*15000/num_lines)
print("Collecting " + str(max_items) + " tweets for each keyword.")

consumer_keys = ["xpisyVgkTPrFUANmPaSdJgUat", "p6tGekq0M3dPEkEWWxiv4DcmH",
                     "u1WuzPHnrPITdITTVsAGrtGxA", "GSskR2nEtbAswvJ3TgCWFsYhx",
                     "MgmMKmSN89nJEDPjZvvrQsILb", "XdVMFQcRGlTdTTuf4KIl3QZXf",
                     "6lBwFbPfNRzCI3Pj2tMBvGN7t", "hG8X0ltdRMYdYa4Q6SBq1wdAG",
                     "zLRd2nMg7DxkS3vKCvnwisZ98", "X7Q9z2pUQEmZ8sZDDUQM7UT1w",
                     "LVRTOlxxRw3gTV9qfoLZG1JPZ", "JOxYuusSGH0YtiWcPZvWhvQ1Q",
                     "Obrv9MJkKlH09clnylxBvQ3lg", "IXmZacmxG2zhQhMxGlmI4wTNH",
                     "qqx98BWM369rLi5tt2g0juMyR", "dI72YOsNnlSx6kjlZkd6C0UEZ",
                     "EDDD52YX3YkY6BTlkTCqrPQvt"]

consumer_secrets = ["C8ytysaHHDGcufScBpfowmCe4z7TYEL0WIVKq6gxvfyl9BePJV",
                    "jGqeSLD5wyU66iNUhlLOMtpNaaNeNXtAuKSJHyf09fUD4r75OJ",
                    "KJ5HycitNPOVQHvl3b0wbYxlrJDcvEQOYbixBknXl8ExmN6vOC",
                    "OJSjRNVrWPZ3hj4etuj2qAW7hr5m2XgRXp64tXpNejIBxCeTm5",
                    "Y8DZbTPi2lbnAxSBKkA5uuZ0qL9VdiuiwZKvlkaPdUPyuW5WLe",
                    "npFNyqylOvTvIj8EyvmF5i9aEOXbEPMbahKJVWmF9sbNe6PwKl",
                    "epo14bnXZcEPW6sPzdejeqFWAFJF7UYPTgpp54Dtxk1RnGDPFc",
                    "mRJbIzXVEBkYj5Q3sOZ0JkCUt5yNuUq4C34LI17W5e6GX2xHvy",
                    "orjFNBiIOaNyCyjXVrwCMnf8W6QlQwEZjkTEZtIJjLWXbKnBPB",
                    "aYdgwg1I0C3ayf0cvk4dLMcyEUA9pwoZlibnB19XYPbmxkLHSF",
                    "l4Ia0rQgEZcMeveYWnFoSE51JhHGm7G4zrJ4ubJJ7cKhmwLk6E",
                    "MGIjrETWZpc4alUoZL59IGep6skA961ZWc1VI4omAsEgvioaac",
                    "Cq0d1cPOOJbEESwKM9IojUjnUtYAlCBDnCanhJtiOdzlDKyCFp",
                    "8yy46nH3cS095M4sTBwJxGg8O6jrWYiCumgtcdbKaPIsrq5jkI",
                    "bjzG0Yewq3X8E5NmSikO99yUfUmiANg5S6NTOjZCfOVIvIhy7T",
                    "7q3yUdMdxuMTFOBDQGu7cnXePR4B4iIWhf9tlXdxygYnaxyKD5",
                    "DoPIemRaXr4GdHxoR7ylQHqIJc4Ic7b0RiSAFPiHaJhSGTP26x"

                    ]
access_tokens = ["938710222138470400-rTN2LYAeupq5kkFrv2wKghbBFriDJ0Y",
                 "938710222138470400-a7R6bc2Z1WlcqKeZNVIlwyJdkgb7EMV",
                 "938710222138470400-rDljCyFOUbavLWQvLDl3QCDAlrTuxIO",
                 "1015247261968633856-oPxLqBY9brju3d4ug0p1TTxoRYRQBk",
                 "1015247261968633856-RGJNM6zbYHFKQRoV51uSdQvuJ3XdCY",
                 "1015247261968633856-kebFk0NXuPJ1PHz7bfwqkP2sVXJLlZ",
                 "106111046-BU1EcWFevJKkSAW7jtyBXJTohXYgu83nKCBeXESe",
                 "106111046-3OdTNvqTRjzs4jcRsgcC1oJt81LsQQwE7I2ZMDZY",
                 "106111046-PguFRm9jHAPnQ3hQdregfjGlWzFo5g3MO3ZpRPMH",
                 "2956391868-Ub7IQOqDoswyiWJQUbTZ8AlLXq3142B7y8jtmup",
                 "2956391868-MbIKbeAy7NlrJcs0dJVWnijtFdppTrI4gknTqkl",
                 "4621868118-FBHOI4NUfaU3lA8gYj3zL2oRDhjqkGhrAMQ6RYV",
                 "4621868118-NJVoqflIWvqQ9fWk5KBQvnIQS4fbVEgNaj8xjcW",
                 "4621868118-AzCnxD1CNa6jGav3GzadegQqwz6NSYVLE07M9SD",
                 "1015246076339580929-qEeixNTX0H2GvSuyYbKydvtv4oYl45",
                 "1015246076339580929-NxiqD9gIDvdaIjWXFxhVqJ1g6uAdyl",
                 "1015246076339580929-6RcrbwQlH0c1FzbTC8YNEytSPXde7v"
                 ]
access_token_secrets = ["lDCio5pVsIWDfYDHs7yNOD1L6WBFEdlWemRjQfipHb9hQ",
                        "y5YDPmTSQUxfZW3gmNynrWdCjZWqYzIQj17DISPfYADI9",
                        "0xbGvThYuyZvEqaLvX2Hx5h4PFCTVci2YSIrwpZamlF6Z",
                        "zQR1b7pRzkxSOsBv2fuCijI4gdJqoXwjHFo89gAWbkjJD",
                        "hvFlSzLzYBkKX3NHXtpABdCItSdOTr3VCQqclDrBnWR8I",
                        "T95k9KBV9YQ0yFUOb0GZiVybonpmxJQYLnTWxBIXcP2Cj",
                        "29C7OuyPGdlUvQx45hSqGn9ce1qSWyddJVV49JF2HL6fz",
                        "synqo2RgdWzlkfXle2vynTt0oasVcIU0uc3iOSFDAO7PZ",
                        "imTTr821KmWCxLDodDVQdeA9yHgL3Ml6BjzgaVZbJn5JG",
                        "jIrUjcsHuYNTZommq00s9RDf78WnWOL65PrNIf5iPCEqU",
                        "VP5vN22xeTedHklbVj9jvcmbLkJcFhDA2y7N4EbW12hld",
                        "fylL8zTuWNIv8vRbvJ8k3msg3MPTsID9D6gc5Gvf1UEQD",
                        "yESs2ecdcfnFSjQVeeWUQiltG8Yl9D6WVvtzIUpfVoSJX",
                        "zhspZWeucbhvq4Y6jfecYjj3Uy8Q26elvfGLKL1GASxp4",
                        "F2sWJU4qR0TBAaZVgOq7SWva6AGhgjzry30jp6bfZn8zr",
                        "exi8U13e0chmUqORSEwdsyOXrUfjHR7IQwO9BZ8DSU5Su",
                        "Dx0XHcIrjUAo7RvInM9QbQELy2e5vHktVQPOudb8Bm4vs"
                        ]



file = open('../keywords.txt', 'r')
count_ins = 0
count_dup = 0

time1 = time.time()

for i, keyword in enumerate(file):
    print(f"Keyword: {i}/{num_lines},    Current Keyword: {keyword}")
    query = keyword

    consumer_key = consumer_keys[f]
    consumer_secret = consumer_secrets[f]
    access_token = access_tokens[f]
    access_token_secret = access_token_secrets[f]

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=False)

    client = pymongo.MongoClient()
    db = client.social_data
    coll = db.posts


    try:
        res = tweepy.Cursor(api.search, q=query, count=max_items, lang="en", tweet_mode="extended").items(max_items)
        """
        for tweet in res:
            __ = _
        """
    except:
        print(f"###############  NOTE: KEY EXPIRED. MOVING TO NEXT KEY: {f+1}")
        f += 1
        if f == 13:
            break

        consumer_key = consumer_keys[f]
        consumer_secret = consumer_secrets[f]
        access_token = access_tokens[f]
        access_token_secret = access_token_secrets[f]

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        api = tweepy.API(auth, wait_on_rate_limit=False)
        res = tweepy.Cursor(api.search, q=query, count=max_items, lang="en",
                            tweet_mode="extended").items(max_items)
    """
            try:
                for m, tweet in enumerate(res):
                    __ = tweet.id_str
            except:
                f += 1
                if f == 13:
                    break
    
                consumer_key = consumer_keys[f]
                consumer_secret = consumer_secrets[f]
                access_token = access_tokens[f]
                access_token_secret = access_token_secrets[f]
    
                auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
                auth.set_access_token(access_token, access_token_secret)
    
                api = tweepy.API(auth, wait_on_rate_limit=True)
                res = tweepy.Cursor(api.search, q=query, count=max_items, lang="en",
                                    tweet_mode="extended").items(max_items)
    """
    try:
        for tweet in res:

            id          = tweet.id_str
            user_id     = tweet.user.id_str
            username    = tweet.user.name
            timestamp   = tweet.created_at.strftime('%d %b %Y')
            in_reply_to = tweet.in_reply_to_user_id
            location    = tweet.user.location
            fav         = tweet.favorite_count
            ht          = tweet.entities["hashtags"]

            hashtags = []
            for k in ht:
                hashtags.append(k["text"])  # List of hashtags.
            try:
                _ = tweet.retweeted_status  # Can't be accessed if not retweet.
                RT = True
            except AttributeError:  # If original tweet
                RT = False
            if RT:
                content = tweet.retweeted_status.full_text
            else:
                content = tweet.full_text

            source      = 'twitter'
            date_added  = datetime.now().strftime('%d %b %Y')
            keyword     = query

            jsonx = {"_id": id, 'keyword': keyword, "username": username, "user_id": user_id, "content": content, "timestamp": timestamp,
                     "location": location, "hashtags": hashtags, "Retweet": RT, "Fav": fav, 'source': source,
                     'date_added': date_added, 'in_reply_to': in_reply_to}

            try:

                coll.insert(jsonx)  # and insert
                count_ins += 1
            except pymongo.errors.DuplicateKeyError:  # Except if it's already there
                count_dup += 1
    except:
        print(f"###############  NOTE: KEY EXPIRED. MOVING TO NEXT KEY: {f+1}")
        f += 1
        if f == 13:
            break

        consumer_key = consumer_keys[f]
        consumer_secret = consumer_secrets[f]
        access_token = access_tokens[f]
        access_token_secret = access_token_secrets[f]

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        api = tweepy.API(auth, wait_on_rate_limit=False)
        res = tweepy.Cursor(api.search, q=query, count=max_items, lang="en",
                            tweet_mode="extended").items(max_items)

        id = tweet.id_str
        user_id = tweet.user.id_str
        username = tweet.user.name
        timestamp = tweet.created_at.strftime('%d %b %Y')
        in_reply_to = tweet.in_reply_to_user_id
        location = tweet.user.location
        fav = tweet.favorite_count
        ht = tweet.entities["hashtags"]
        hashtags = []
        for k in ht:
            hashtags.append(k["text"])  # List of hashtags.
        try:
            _ = tweet.retweeted_status  # Can't be accessed if not retweet.
            RT = True
        except AttributeError:  # If original tweet
            RT = False
        if RT:
            content = tweet.retweeted_status.full_text
        else:
            content = tweet.full_text

        source = 'twitter'
        date_added = datetime.now().strftime('%d %b %Y')
        keyword = query

        jsonx = {"_id": id, 'keyword': keyword, "username": username, "user_id": user_id, "content": content,
                 "timestamp": timestamp,
                 "location": location, "hashtags": hashtags, "Retweet": RT, "Fav": fav, 'source': source,
                 'date_added': date_added, 'in_reply_to': in_reply_to}
        # print(jsonx)
        try:
            # print("reached")
            coll.insert(jsonx)  # and insert
            count_ins += 1
        except pymongo.errors.DuplicateKeyError:  # Except if it's already there
            count_dup += 1

time2 = time.time()
print()
print("Time taken = {} mins.".format((time2 - time1) / 60))
print(f"######Inserted: {count_ins}, Duplicates Discarded: {count_dup} #######")
