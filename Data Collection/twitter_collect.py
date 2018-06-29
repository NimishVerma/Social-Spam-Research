import tweepy
import re
import pymongo
from datetime import datetime
from datetime import timedelta


def search(query, max_items=200, startdate=None, enddate=None, f=0):

    consumer_keys = ['AYDNqr9ycBI9qaOWoYXJgYnKY', 'dEj9PuikWT7amd5Ud5k79jurB', "p6tGekq0M3dPEkEWWxiv4DcmH", "A9TNLwxvBUF52slanuQMbbN4i","yKiP0AySVUpebAEqCTh8iYCjJ","QMqKeFUK3t86LdeWXkKEdHUT2","rHQ3rAswzTlerA4TCpFZDGota", "eet9COlNWAmhbQ93Qtw1N5qEo", "SBlZsM7ckoeECJtO6KSyfGscz", "tGLPkm7apTos0ObiYYX5Npvru", "w3oZSQnGxnpgZpME6VJyyFLBT", "GOVoDqdd4c9mC8Sr0l9nVAGuc", "dEj9PuikWT7amd5Ud5k79jurB"]
    consumer_secrets = ['tSHVPOr6JrQ9KNnqX1aSOGnKecmPeQQ37j81JAURI0t6deb8AA', '5nGkmWffNKIepE5bKjzj6RNLn0RTjjjLgxeVFp19ZLFwcOP40H', "jGqeSLD5wyU66iNUhlLOMtpNaaNeNXtAuKSJHyf09fUD4r75OJ", "bahZEJNjhzEWRCVjW7xNMQRY9QCrKJBbKX97gleICO2KckqECm", "uIU1c1UntpU9SG3TSfKqkNhElWFHY7nw4WOjefaGNAOBaW5Baz", "G4ALtFOvrItBtXb3oW3runp5pUKtG7GNMebtowQH8twvXviXcI", "vR0usxrO4nDtyppdGbS48mlc4VenYqMBI235l8OKjXVytsbVYv", "WNrioh6Ufi7lsJJxiP8gYDg3uvaJvJPi2ZE4vG17PfxpjL9PFA", "rmkUPcEAWxi51ORC8O7NeujZIkZ0t3JSDyn8bkc7dtinDPDViS", "hbIUh2lqv4VibGqbQexN4CnuQmcKDTcedXJ1SDRWpPwQyyTo4H", "xFMNW2Xjz68maaFqpIGgSyPugtHzZan01mmZT4bWdPGypk2DU0", "YhJfSGIQVAialn57VaCEoJDt6Mm3QEyN57T6QkGt8488wveHVQ", "5nGkmWffNKIepE5bKjzj6RNLn0RTjjjLgxeVFp19ZLFwcOP40H"]
    access_tokens = ['2493864625-SSCalZj2of8hITNgrv4gMvNZX7seGTSWD2MQI0H', '895015384507498496-drN0ZC4fAcKZWG0uk4g1ACQvILaUECZ', "938710222138470400-dzntQAZ2iHqeCSDs9lVX9WO3SWLcLsH", "767202674-wic4r5VrnMwj6Wd82pfOmdwRUXndkh0eRizMt85H", "767202674-bUPVAz4mFB01jXeQrO9MWa2UqG4LLcrverRF6kTS", "767202674-5qIMo6VNUYwD2mvx938jmVgIBdnaNichLqlqGET5", "767202674-DXI7QMAnXDI9R5T7i5fqn4pfY2HWSeabxa8IZwlQ", "540132172-MvnOhI7dCQFYyLsuHrQ9qfBfU50v4vjnYqsrbJ9m", "540132172-G5uhArRvk7rZlRwkKDaiQVS69nITODidQrnSePmp", "540132172-nf0mglx8a5rTl8X71rJt0aYRFQO4KQBZyCXKlStj", "895015384507498496-Te9NDjdEGJCkBZRC0ctcX2dHEOw1fPL", "895015384507498496-06X4sOhT1L1F9wgrIDOop8NOQNwdgvl", "895015384507498496-drN0ZC4fAcKZWG0uk4g1ACQvILaUECZ"]
    access_token_secrets = ['5JBwqg2jWjijXgIRpEsdLrGRYWEuNmc4M0ibfRL2xzrhd', 'rcpKoy7OnipvbnblpPwVamPVvJKoQGt5Ux88sUz1tn0Oj', '3wOdVSudgZPPnc0CfcRJNvPttkx7HtTjvwyvNsz0JFGAD', "VfnhH8XxCnQJ7VfKWZbBpaP39EnuSlwsMcmRMt9512yX7", "Le9Xi3c4VHDg20a30Axa7PBXqGvBUoBE6eEqhONGtHMla", "0HLmWpmx84LeHHTjboOq0IZ9KzmeaUxpTlU6wXWjnZwJz", "v2rLvodmf9LxyAQtn0vVn76XYymaEz9lU2oywKuffmCtg", "En2iXEzONr0E3yCejwkrqHTOnD2KDRJ2WvRb8QRzqrb4t", "F0XVqmvKYxReEnjzMcKOq5rX8EAMCUBTWKomODuEbZNKP", "nwffryYyMEMni3xOA6SSylmgKJj8TsBvkzT3M8PkP5F4n", "aowhQRJbu8zNU5VntMFoKIaCEp0A4iY96Tvbwd8s3MrRr", "Dq7jEKzUcLln92jFRkj8bOraIsRoc0JHBnCmP9Ivzz29E", "rcpKoy7OnipvbnblpPwVamPVvJKoQGt5Ux88sUz1tn0Oj"]

    consumer_key = consumer_keys[f]
    consumer_secret = consumer_secrets[f]
    access_token = access_tokens[f]
    access_token_secret = access_token_secrets[f]

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    client = pymongo.MongoClient()
    db = client.social_data
    coll = db.posts

    startdate = datetime.now()
    enddate = datetime.now()
    start_date = (startdate - timedelta(days=10)).strftime('%Y-%m-%d')
    end_date = (enddate - timedelta(days=9)).strftime('%Y-%m-%d')

    try:  # optional removed params: , since=start_date, until=end_date
        res = tweepy.Cursor(api.search, q=query, count=200, since=start_date, until=end_date, lang="en", tweet_mode="extended").items(max_items)
    except tweepy.error.TweepError:
        f += 1
        if f == 13:
            f = 0
        search(query, max_items, startdate=None, enddate=None, f=f)

    for i, tweet in enumerate(res):

        id          = tweet.id_str
        user_id     = tweet.user.id_str
        username    = tweet.user.name
        timestamp   = tweet.created_at.strftime('%d %b %Y')
        in_reply_to = tweet.in_reply_to_user_id
        location    = tweet.user.location
        fav         = tweet.favorite_count
        ht          = tweet.entities["hashtags"]

        hashtags = []
        for i in ht:
            hashtags.append(i["text"])  # List of hashtags.
        try:
            _ = tweet.retweeted_status  # Can't be accessed if not retweet.
            RT = True
        except AttributeError:  # If original tweet
            RT = False
        if RT:
            content = tweet.retweeted_status.full_text
            # fav = -1  # Fav count only in case of original tweets.
        else:
            content = tweet.full_text

        source      = 'twitter'
        date_added  = datetime.now().strftime('%d %b %Y')
        keyword = query

        jsonx = {"_id": id, 'keyword': keyword, "username": username, "user_id": user_id, "content": content, "timestamp": timestamp,
                 "location": location, "hashtags": hashtags, "Retweet": RT, "Fav": fav, 'source': source,
                 'date_added': date_added, 'in_reply_to': in_reply_to}

        print(jsonx)
        print()


search("call")


