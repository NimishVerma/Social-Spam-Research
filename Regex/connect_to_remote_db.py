
import pymongo

### -h ds241121.mlab.com:41121 -d social_data -c posts -u user -p pass1234 --file test.json

connection_params = {
    'user': 'user',
    'password': 'pass1234',
    'host': 'ds241121.mlab.com',
    'port': 41121,
    'namespace': 'social_data',
}

connection = pymongo.MongoClient(
    'mongodb://{user}:{password}@{host}:'
    '{port}/{namespace}'.format(**connection_params)
)

db = connection.social_data

print(db.collection_names())
coll = db.posts

