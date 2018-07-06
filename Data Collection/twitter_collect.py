import tweepy
import re
import pymongo
from datetime import datetime
from datetime import timedelta

f = 0
# def search(query, max_items=200, f=0, startdate=None, enddate=None):
    # global f

file = open('../keywords.txt', 'r')

num_lines = 0
for line in file:
    if line.strip():
        num_lines += 1

count_ins = count_dup = 0
print("Collecting " + str(int(130000/num_lines)) + " tweets for each keyword.")
max_items = int(130000/num_lines)

file = open('../keywords.txt', 'r')

for i, keyword in enumerate(file):
    print(f"Keyword: {i}/{num_lines},    Current Keyword: {keyword}")
    query = keyword

    count_ins = 0
    count_dup = 0
    consumer_keys = ["xpisyVgkTPrFUANmPaSdJgUat", "p6tGekq0M3dPEkEWWxiv4DcmH",
                     "u1WuzPHnrPITdITTVsAGrtGxA", "GSskR2nEtbAswvJ3TgCWFsYhx",
                     "MgmMKmSN89nJEDPjZvvrQsILb", "XdVMFQcRGlTdTTuf4KIl3QZXf",
                     "6lBwFbPfNRzCI3Pj2tMBvGN7t", "hG8X0ltdRMYdYa4Q6SBq1wdAG",
                     "zLRd2nMg7DxkS3vKCvnwisZ98", "X7Q9z2pUQEmZ8sZDDUQM7UT1w",
                     "LVRTOlxxRw3gTV9qfoLZG1JPZ",

                     ]
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
                        "l4Ia0rQgEZcMeveYWnFoSE51JhHGm7G4zrJ4ubJJ7cKhmwLk6E"

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
                            "VP5vN22xeTedHklbVj9jvcmbLkJcFhDA2y7N4EbW12hld"
                            ]

    consumer_key = consumer_keys[f]
    consumer_secret = consumer_secrets[f]
    access_token = access_tokens[f]
    access_token_secret = access_token_secrets[f]

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=False)

    client = pymongo.MongoClient()
    db = client.social_data
    coll = db.temp


    try:
        res = tweepy.Cursor(api.search, q=query, count=max_items, lang="en", tweet_mode="extended").items(max_items)
        for i, _ in enumerate(res):
            __ = _
    except:
        print(f"###############  NOTE: KEY EXPIRED. MOVING TO NEXT KEY: {f+1}")
        f += 1
        if f == 13:
            break
        consumer_keys = ['AYDNqr9ycBI9qaOWoYXJgYnKY', 'dEj9PuikWT7amd5Ud5k79jurB', "p6tGekq0M3dPEkEWWxiv4DcmH",
                         "A9TNLwxvBUF52slanuQMbbN4i", "yKiP0AySVUpebAEqCTh8iYCjJ", "QMqKeFUK3t86LdeWXkKEdHUT2",
                         "rHQ3rAswzTlerA4TCpFZDGota", "eet9COlNWAmhbQ93Qtw1N5qEo", "SBlZsM7ckoeECJtO6KSyfGscz",
                         "tGLPkm7apTos0ObiYYX5Npvru", "w3oZSQnGxnpgZpME6VJyyFLBT", "GOVoDqdd4c9mC8Sr0l9nVAGuc",
                         "dEj9PuikWT7amd5Ud5k79jurB"]
        consumer_secrets = ['tSHVPOr6JrQ9KNnqX1aSOGnKecmPeQQ37j81JAURI0t6deb8AA',
                            '5nGkmWffNKIepE5bKjzj6RNLn0RTjjjLgxeVFp19ZLFwcOP40H',
                            "jGqeSLD5wyU66iNUhlLOMtpNaaNeNXtAuKSJHyf09fUD4r75OJ",
                            "bahZEJNjhzEWRCVjW7xNMQRY9QCrKJBbKX97gleICO2KckqECm",
                            "uIU1c1UntpU9SG3TSfKqkNhElWFHY7nw4WOjefaGNAOBaW5Baz",
                            "G4ALtFOvrItBtXb3oW3runp5pUKtG7GNMebtowQH8twvXviXcI",
                            "vR0usxrO4nDtyppdGbS48mlc4VenYqMBI235l8OKjXVytsbVYv",
                            "WNrioh6Ufi7lsJJxiP8gYDg3uvaJvJPi2ZE4vG17PfxpjL9PFA",
                            "rmkUPcEAWxi51ORC8O7NeujZIkZ0t3JSDyn8bkc7dtinDPDViS",
                            "hbIUh2lqv4VibGqbQexN4CnuQmcKDTcedXJ1SDRWpPwQyyTo4H",
                            "xFMNW2Xjz68maaFqpIGgSyPugtHzZan01mmZT4bWdPGypk2DU0",
                            "YhJfSGIQVAialn57VaCEoJDt6Mm3QEyN57T6QkGt8488wveHVQ",
                            "5nGkmWffNKIepE5bKjzj6RNLn0RTjjjLgxeVFp19ZLFwcOP40H"]
        access_tokens = ['2493864625-SSCalZj2of8hITNgrv4gMvNZX7seGTSWD2MQI0H',
                         '895015384507498496-drN0ZC4fAcKZWG0uk4g1ACQvILaUECZ',
                         "938710222138470400-dzntQAZ2iHqeCSDs9lVX9WO3SWLcLsH",
                         "767202674-wic4r5VrnMwj6Wd82pfOmdwRUXndkh0eRizMt85H",
                         "767202674-bUPVAz4mFB01jXeQrO9MWa2UqG4LLcrverRF6kTS",
                         "767202674-5qIMo6VNUYwD2mvx938jmVgIBdnaNichLqlqGET5",
                         "767202674-DXI7QMAnXDI9R5T7i5fqn4pfY2HWSeabxa8IZwlQ",
                         "540132172-MvnOhI7dCQFYyLsuHrQ9qfBfU50v4vjnYqsrbJ9m",
                         "540132172-G5uhArRvk7rZlRwkKDaiQVS69nITODidQrnSePmp",
                         "540132172-nf0mglx8a5rTl8X71rJt0aYRFQO4KQBZyCXKlStj",
                         "895015384507498496-Te9NDjdEGJCkBZRC0ctcX2dHEOw1fPL",
                         "895015384507498496-06X4sOhT1L1F9wgrIDOop8NOQNwdgvl",
                         "895015384507498496-drN0ZC4fAcKZWG0uk4g1ACQvILaUECZ"]
        access_token_secrets = ['5JBwqg2jWjijXgIRpEsdLrGRYWEuNmc4M0ibfRL2xzrhd',
                                'rcpKoy7OnipvbnblpPwVamPVvJKoQGt5Ux88sUz1tn0Oj',
                                '3wOdVSudgZPPnc0CfcRJNvPttkx7HtTjvwyvNsz0JFGAD',
                                "VfnhH8XxCnQJ7VfKWZbBpaP39EnuSlwsMcmRMt9512yX7",
                                "Le9Xi3c4VHDg20a30Axa7PBXqGvBUoBE6eEqhONGtHMla",
                                "0HLmWpmx84LeHHTjboOq0IZ9KzmeaUxpTlU6wXWjnZwJz",
                                "v2rLvodmf9LxyAQtn0vVn76XYymaEz9lU2oywKuffmCtg",
                                "En2iXEzONr0E3yCejwkrqHTOnD2KDRJ2WvRb8QRzqrb4t",
                                "F0XVqmvKYxReEnjzMcKOq5rX8EAMCUBTWKomODuEbZNKP",
                                "nwffryYyMEMni3xOA6SSylmgKJj8TsBvkzT3M8PkP5F4n",
                                "aowhQRJbu8zNU5VntMFoKIaCEp0A4iY96Tvbwd8s3MrRr",
                                "Dq7jEKzUcLln92jFRkj8bOraIsRoc0JHBnCmP9Ivzz29E",
                                "rcpKoy7OnipvbnblpPwVamPVvJKoQGt5Ux88sUz1tn0Oj"]

        consumer_key = consumer_keys[f]
        consumer_secret = consumer_secrets[f]
        access_token = access_tokens[f]
        access_token_secret = access_token_secrets[f]

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        api = tweepy.API(auth, wait_on_rate_limit=False)
        res = tweepy.Cursor(api.search, q=query, count=max_items, lang="en",
                            tweet_mode="extended").items(max_items)

        # search(query, max_items, f=f, startdate=None, enddate=None)

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
