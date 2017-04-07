#% spark.pyspark

from datetime import datetime, timedelta
import re
from requests import get  ## Not part of BI install- must install manually

def process_response(r, write_flag = "w"):
    DATA_DIR = "/home/rawkintrevo/gits/ffsf17-twitter-recos/data"
    for tweet in r.json()['tweets']:
        try:
            id = tweet['message']['id'].split(":")[-1]
            if "location" in tweet['message']['actor']:
                local = re.sub(r'\W+', '', tweet['message']['actor']['location']['displayName']).lower()
                if local == "":
                    local = "None"
            else:
                local = "None"
            with open(DATA_DIR + "/user-local.csv", write_flag) as ul:
                line = id + "," + local
                ul.write(line + '\n')
            ########################################################################################
            words = [re.sub(r'\W+', '', w).lower() for w in tweet['message']['body'].split()] # list
            with open(DATA_DIR + "/user-words.csv", write_flag) as uw:
                for w in words:
                    if "http" in w:
                        continue
                    line = id + "," + w
                    #print "word", line
                    uw.write(line+ '\n')
            # ########################################################################################
            hashtags = tweet['message']['twitter_entities']['hashtags'] # list
            clean_hashtags = [re.sub(r'\W+', '', ht['text']).lower() for ht in hashtags if not re.sub(r'\W+', '', ht['text']).lower() == ""]
            if len(hashtags) > 0:
                line = "\n".join([id + "," + cht for cht in clean_hashtags])
                if line == "":
                    line = id + ",None"
            else:
                line = id + ",None"
            with open(DATA_DIR + "/user-ht.csv", write_flag) as uh:
                uh.write(line + "\n")
            ########################################################################################
            friends = [f['id'] for f in tweet['message']['twitter_entities']['user_mentions']] #list
            if len(friends) > 0:
                line = "\n".join([id + "," + str(f) for f in friends])
            else:
                line = id + ",None"
            with open(DATA_DIR + "/user-friends.csv", write_flag) as uf:
                uf.write(line + "\n")

            write_flag = "a"
        except Exception as e:
            print e
            print tweet


process_response(r)
# Creds for IBM Twitter Service

creds = {
    "credentials": {
        "username": "d01ec50e-f5e4-4574-909d-d1c0e9b4f586",
        "password": "vcUW8bidL6",
        "host": "cdeservice.mybluemix.net",
        "port": 443,
        "url": "https://d01ec50e-f5e4-4574-909d-d1c0e9b4f586:vcUW8bidL6@cdeservice.mybluemix.net"
    }
}

endpoint = "/api/v1/messages/search"
base_url = "https://%s:%s@cdeservice.mybluemix.net:%i%s" % (creds['credentials']['username'],
                                                            creds['credentials']['password'],
                                                            creds['credentials']['port'], endpoint)
payload = {
    'size': 500,  # how many records to fetch
    #   'from' : "", #starting record
    'q': 'pizza'
}

r = get(base_url, payload)

process_response(r)

max_tweets = 500
tweets_collected = payload['size']

while True:
    if 'next' in r.json()['related'] and tweets_collected < max_tweets:
        r = get(base_url, payload)
        process_response(r, 'a')
        tweets_collected += payload['size']
        print tweets_collected
    else:
        print
        break

print tweets_collected
