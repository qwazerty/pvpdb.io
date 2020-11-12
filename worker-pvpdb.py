#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import json
import time
import datetime
import requests
import signal
import pymongo
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from oauthlib.oauth2 import TokenExpiredError

debug = False

try:
    import tokens
except ImportError:
    print("[ERROR] Create a credentials file tokens.py with:")
    print("[ERROR]   tokens = {")
    print("[ERROR]     'pvpdb-worker-1': {")
    print("[ERROR]       'client_id': 'CLIENT_ID',")
    print("[ERROR]       'client_secret': 'CLIENT_SECRET'")
    print("[ERROR]     }")
    print("[ERROR]   }")
    print("[ERROR]   mongo_url = mongodb://USER:PASS@localhost:27017/")
    print("[ERROR]")
    print("[ERROR] To redeem IDs, check https://develop.battle.net/access/")
    sys.exit(1)

if len(sys.argv) <= 1:
    print("[ERROR] Usage: workers-pvpdb.py <tokenid> [action] [region] [faction]")
    sys.exit(1)

token_url = 'https://eu.battle.net/oauth/token'
client_id = tokens.tokens[sys.argv[1]]['client_id']
client_secret = tokens.tokens[sys.argv[1]]['client_secret']

client = BackendApplicationClient(client_id=client_id)
def oauth_login(client):
    oauth = OAuth2Session(client=client)
    return oauth.fetch_token(token_url=token_url, client_id=client_id, client_secret=client_secret)

slug_url = "https://{region}.api.blizzard.com/data/wow/realm/index?namespace={namespace}"
pvp_summary_url = "https://{region}.api.blizzard.com/profile/wow/character/{realm}/{character}/pvp-summary?namespace={namespace}"
token = oauth_login(client)
character_days_ttl = 90

client = pymongo.MongoClient(tokens.mongo_url)
pvpdb = client['pvpdb']

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self,signum, frame):
    self.kill_now = True

def oauth_api_call(url):
    if debug:
        print("[DEBUG] oauth_api_call({})".format(url))
    global token
    time.sleep(0.2)
    try:
        headers = {"Authorization": "Bearer " + token['access_token']}
        res = requests.get(url, headers=headers)
        if (res.status_code == 401):
            token = oauth_login(client)
            headers = {"Authorization": "Bearer " + token['access_token']}
            res = requests.get(url, headers=headers)
        return res
    except:
        print("[WARN] > ConnectionError. Retrying...")
        print("[WARN] > {}".format(url))
        res = requests.get(url, headers=headers)
        if (res.status_code == 200):
            return json.loads(res.text)
        else:
            return {}

def generate_realm_slug(file):
    with open(file, "r") as f:
        data = f.read()
        data = data.replace("local _, ns = ...", "")
        data = data.replace("ns.realmSlugs = ", "")
        data = data.replace("[", "")
        data = data.replace("]", "")
        data = data.replace(" =", ":")
        data = data.replace(",\n}", "}")
    return json.loads(data)

def generate_realm_slug_test():
    namespace = "dynamic-eu"
    realm_res = oauth_api_call(slug_url.format(region="eu", namespace=namespace))
    if realm_res.status_code == 200:
        realm_json = json.loads(res.text)
        realms = {r['name']['en_US']:r['slug'] for r in realm_json['realms']}
        print(realms)
        return realms
    else:
        print("[ERROR] Could not generate realms_slug.")
        print("[ERROR] [{code}] {text}".format(code=res.status_code, text=res.text))
        return {}

def get_characters_list(file):
    characters = {}
    with open(file, "r") as f:
        for line in f:
            if ("F = function()" in line):
                r = re.split('"', line)[1]
                c = re.split('{|}', line)[1].replace('"', '').split(",")[1:]
                characters.update({r: c})
    return characters

def init_characters(db_characters, region, faction):
    characters = get_characters_list("rio/db_{region}_{faction}_characters.lua".format(region=region, faction=faction))
    db_characters.create_index([('lastModified', pymongo.ASCENDING)])
    db_characters.create_index([('name', pymongo.ASCENDING)])
    db_characters.create_index([('realm', pymongo.ASCENDING)])
    db_characters.create_index([('name', pymongo.ASCENDING), ('realm', pymongo.ASCENDING)], unique=True)
    for realm in characters:
        print("[INFO] Init {region}-{faction}-{realm}".format(region=region, faction=faction, realm=realm))
        doc = [ { "name": c, "realm": realm, "lastModified": None } for c in characters[realm] if db_characters.find_one({"name": c, "realm": realm}) is None and realm in realm_slug ]
        if len(doc) == 0:
            print("[INFO] 0 documents to insert, skipping")
        else:
            res = db_characters.insert_many(doc)
            if res.acknowledged:
                print("[INFO] Inserted {} documents".format(len(doc)))
            else:
                print("[ERROR] Could not insert {} documents".format(len(doc)))

def get_pvp_summary(doc, region):
    namespace = "profile-{region}".format(region=region)
    if debug:
        print("[DEBUG] get_pvp_summary({doc}, {namespace})".format(doc=doc, namespace=namespace))
    res = oauth_api_call(pvp_summary_url.format(region=region, realm=realm_slug[doc['realm']], character=doc['name'].lower(), namespace=namespace))
    if res.status_code == 200:
        stats_json = json.loads(res.text)
        if 'honor_level' in stats_json:
            doc.update({
                'honor_level': stats_json['honor_level']
            })
        if 'brackets' in stats_json:
            for bracket in stats_json['brackets']:
                stats_bracket = oauth_api_call(bracket['href'].replace('http://', 'https://'))
                if 'bracket' in stats_bracket:
                    doc.setdefault('pvp-bracket', {})
                    doc['pvp-bracket'].update({
                        stats_bracket['bracket']['type']: {
                            "current_rating": stats_bracket['rating'],
                            "season_match_statistics" : {
                                "played": stats_bracket['season_match_statistics']['played'],
                                "won": stats_bracket['season_match_statistics']['won'],
                                "lost": stats_bracket['season_match_statistics']['lost'],
                            }
                        }
                    })
    elif res.status_code == 404:
        print("[WARN] Characters {region}-{realm}-{name} not found".format(region=region, realm=doc['realm'], name=doc['name']))
        return False
    else:
        print("[ERROR] Unexpected error for {region}-{realm}-{name}".format(region=region, realm=doc['realm'], name=doc['name']))
        print("[ERROR] [{code}] {text}".format(code=res.status_code, text=res.text))
        return True
    return True

def update_characters(db_characters, region, faction):
    killer = GracefulKiller()
    while not killer.kill_now:
        doc = db_characters.find_one_and_update(
            {"lastModified": None},
            {"$currentDate": {"lastModified": True}}
        )
        if doc == None:
            print("[INFO] No update found for {region} {faction}".format(region=region, faction=faction))
            break
        if doc['realm'] not in realm_slug:
            print("[WARN] Realm not found for {region}-{faction}-{realm}-{name}".format(region=region, faction=faction, realm=doc['realm'], name=doc['name']))
            db_characters.remove({"_id": doc['_id']})
            continue
        if get_pvp_summary(doc, region):
            del doc['lastModified']
            res = db_characters.update_one(
                {"_id": doc['_id']},
                {
                    "$set": doc,
                    "$currentDate": {"lastModified": True}
                }
            )
            if res.acknowledged:
                print("[INFO] Updated {}".format(doc))
            else:
                print("[ERROR] Mongo error for update {}".format(doc))
        else:
            print("[WARN] Deleting {}".format(doc))
            db_characters.remove({"_id": doc['_id']})

    if killer.kill_now:
        print("[INFO] Graceful shutdown...")
        sys.exit(0)

def main():
    if len(sys.argv) >= 3 and sys.argv[2] == "init":
        for r in ["eu", "us", "kr", "tw"]:
            for f in ["alliance", "horde"]:
                init_characters(pvpdb['characters_{r}_{f}'.format(r=r, f=f)], r, f)
    elif len(sys.argv) >= 3 and sys.argv[2] == "update":
        for r in ["eu", "us", "kr", "tw"]:
            for f in ["alliance", "horde"]:
                update_characters(pvpdb['characters_{r}_{f}'.format(r=r, f=f)], r, f)

realm_slug = generate_realm_slug('rio/db_realms.lua')
main()
