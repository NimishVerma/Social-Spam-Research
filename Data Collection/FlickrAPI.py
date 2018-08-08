import sys
f = 0
def to_run(inp1,inp2):
    
    import requests

    API_KEYS = ['3ecafd97694e35054e50cae6316861e1',
                'a350cfd7c4402a5612cf05d472f04562',
                '304fad5bfc51c79cf44da91a7a29ad83',
                '8c0734664bbfabe9e44dfd62331c0f7e',
                '2e0ffb69dd39dd9746a78c394d4d5cdb',
                'cfcc76ee8836a348e1246aee73e7a787']

    API_SECRET = ['9c8f38a0dfaabcde',
                'd9b30f2b30ce9837',
                '72551a2ca8252487',
                '1001d462c311eaf7',
                '1baa49b0e3beaae2',
                '590a261f1535947d']

    from time import strftime

    import pymongo
    from pymongo import MongoClient

    import json
    url = 'https://api.flickr.com/services/rest/?'
    # ID should be of 16 digits


    import datetime
    now_date = datetime.datetime.now()
    delta = datetime.timedelta(days=1)
    min_date = now_date - delta

    # Function to make request and obtain a response
    def search(query):
        global f
        id_list = []
        result = []
        # Code for retreiving photo IDs
        # looping over 20 pages
        coll = None
        client = MongoClient()
        db = client.social_data
        coll = db.comments
        payload = {
                    'api_key':str(API_KEYS[f]),
                    'format':'json',
                    'method': 'flickr.photos.search',
                    'page':1,
                    'per_page':500,
                    'text':str(query),
                    'min_upload_date': min_date
                }


        resp = requests.post(url, data=payload, timeout=100)
        resp = resp.text

        # cleaning response
        resp = resp[14:-1]

        # response to json
        resp = json.loads(resp)
        resp = resp['photos']['photo']

        print("ID LIST LENGTH : {} \nResp Length : {}".format(len(id_list),len(resp)))

        #looping over all list of photo details to obtain attribute 'id'
        for j in range(len(resp)):
            id_list.append(resp[j]['id'])

        import datetime
        time2 = datetime.datetime.now()
        print("New ID LIST LENGTH: {} ".format(len(id_list)))
        #removing duplicate IDs
        id_set = set(id_list)
        id_list = list(id_set)
        print("ID LENGTH AFTER DUPLICATE REMOVAL: {}".format(len(id_list)))
            
            
        count = 0
        result = []
        keyword = query
        failed = 0
        
        # Loop for every photo ID
        from datetime import datetime

        for count,idx in enumerate(id_list):

            payload = {
                            'api_key': str(API_KEYS[f]),
                            'format': 'json',
                            'method': 'flickr.photos.comments.getList',
                            'photo_id': idx
                        }
            try:
                resp = requests.post(url, data=payload, timeout=100)
            except:
                print(resp)
                print('jsonDecodeError in comment request'+ '\n Changing key')
                if(f == 4):
                    f = 0
                f += 1
                print(f"Key value:{f}")
                continue
                
            try:
                resp = resp.text
                # cleaning response 
                resp = resp[14:-1]
                resp = json.loads(resp)   
            except:
                print(resp)
                print('jsonDecodeError in comment request.')
                continue
                


            # Code for retreiving location of photo
            payload_loc = {
                            'api_key':str(API_KEYS[f]),
                            'photo_id':idx,
                            'method':'flickr.photos.geo.getLocation',
                            'format':'json'
            }
            try:
                resp_loc = requests.post(url, data=payload_loc)
                resp_loc = resp_loc.text[14:-1]
                resp_loc = json.loads(resp_loc)
            except:
                print("APIKey error, Changing key..")
                if(f == 4):
                    f = 0
                f += 1
                continue
            
            try:
                long = resp_loc['photo']['location']['longitude']
                lat = resp_loc['photo']['location']['latitude']
                loc = [long,lat]
            except:
                loc = -1
                continue
            
            #Code to retrieve count of favorites of the photo

            # Code for retrieving all other details from comment response
            try:
                
                print(len(resp['comments']['comment']), '\t', count)
                for x in range(len(resp['comments']['comment'])):
                    
                    comm = resp['comments']['comment'][x]['_content']
                    comm_id = resp['comments']['comment'][x]['id']  
                    author = resp['comments']['comment'][x]['authorname']
                    userid = resp['comments']['comment'][x]['author']
                    timestamp = resp['comments']['comment'][x]['datecreate']
                    timestamp = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
                    
                    d = {'id':comm_id,
                        'keyword':query,
                        'username':author,
                        'user_id':userid,
                        'content':comm,
                        'timestamp':str(timestamp),
                        'location':loc,
                        'hashtag':-1,
                        'RT':-1,
                        'fav':-1,
                        'source':'flickr',
                        'date_added':str(now_date)[:11],
                        'in-reply-to':-1
                        }
                    coll.insert_one(d)
                    result.append(d)
                
            except:
                failed += 1

            # displaying progress while looping

        

        time3 = datetime.now()    
        print(f"ID retrieval time: {time2 - now_date}")
        print(f"Comment retrieval time: {time3 - time2}")
        print("Keyword {} searched. {} entries found.".format(query, len(result)))
        return result
    file = open('keywords.txt', 'r')
    file = file.read()
    file = file.split('\n')

    entries = 0
    for x in file[inp1:inp2]:
        result = search(x)
        entries += len(result)
    print("\n -----PROCESS FINISHED----- \n")
    print(f"Entries added : {entries}")

if __name__ == "__main__":
    a = int(sys.argv[1])
    b = int(sys.argv[2])
    to_run(a,b)