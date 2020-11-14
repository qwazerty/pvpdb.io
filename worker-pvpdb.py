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
slug_url = "https://{region}.api.blizzard.com/data/wow/realm/index?namespace={namespace}"
pvp_summary_url = "https://{region}.api.blizzard.com/profile/wow/character/{realm}/{character}/pvp-summary?namespace={namespace}"
token_url = 'https://eu.battle.net/oauth/token'
character_days_ttl = 90

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True

class Oauth:
    client_id = None
    client_secret = None
    oauth_client = None
    token = None

    def oauth_login(self, client):
        oauth = OAuth2Session(client=client)
        return oauth.fetch_token(token_url=token_url, client_id=self.client_id, client_secret=self.client_secret)

    def oauth_api_call(self, url):
        if debug:
            print("[DEBUG] oauth_api_call({})".format(url))
        time.sleep(0.2)
        try:
            headers = {"Authorization": "Bearer " + self.token['access_token']}
            res = requests.get(url, headers=headers)
            if (res.status_code == 401):
                self.token = self.oauth_login(self.oauth_client)
                headers = {"Authorization": "Bearer " + self.token['access_token']}
                res = requests.get(url, headers=headers)
            return res
        except:
            print("[WARN] > ConnectionError. Retrying...")
            print("[WARN] > {}".format(url))
            res = requests.get(url, headers=headers)
            return res

    def __init__(self):
        # Check token file
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
        self.client_id = tokens.tokens[sys.argv[1]]['client_id']
        self.client_secret = tokens.tokens[sys.argv[1]]['client_secret']
        self.oauth_client = BackendApplicationClient(client_id=self.client_id)
        self.token = self.oauth_login(self.oauth_client)

class Mongo:
    db = None

    def __init__(self):
        # Check token file
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
        self.db = pymongo.MongoClient(tokens.mongo_url)

class Worker:
    mongo = None
    oauth = None
    realm_slug = None

    def logger(self, msg, newline=True):
        print(" "*os.get_terminal_size()[0], end='\r')
        print(msg, end=('\n' if newline else '\r'))

    def generate_realm_slug(self, file):
        with open(file, "r") as f:
            data = f.read()
            data = data.replace("local _, ns = ...", "")
            data = data.replace("ns.realmSlugs = ", "")
            data = data.replace("[", "")
            data = data.replace("]", "")
            data = data.replace(" =", ":")
            data = data.replace(",\n}", "}")
        return json.loads(data)

    def get_characters_list(self, file):
        characters = {}
        with open(file, "r") as f:
            for line in f:
                if ("F = function()" in line):
                    r = re.split('"', line)[1]
                    c = re.split('{|}', line)[1].replace('"', '').split(",")[1:]
                    characters.update({r: c})
        return characters

    def init_characters(self, region, faction):
        characters = self.get_characters_list("rio/db_{region}_{faction}_characters.lua".format(region=region, faction=faction))
        db_characters = self.mongo.db['pvpdb']['characters_{r}_{f}'.format(r=region, f=faction)]
        db_characters.create_index([('lastModified', pymongo.ASCENDING)])
        db_characters.create_index([('name', pymongo.ASCENDING)])
        db_characters.create_index([('realm', pymongo.ASCENDING)])
        db_characters.create_index([('name', pymongo.ASCENDING), ('realm', pymongo.ASCENDING)], unique=True)
        for realm in characters:
            self.logger("[INFO] Init {region}-{faction}-{realm}".format(region=region, faction=faction, realm=realm))
            doc = [ { "name": c, "realm": realm, "lastModified": None } for c in characters[realm] if db_characters.find_one({"name": c, "realm": realm}) is None and realm in self.realm_slug ]
            if len(doc) == 0:
                print("[INFO] 0 documents to insert, skipping")
            else:
                res = db_characters.insert_many(doc)
                if res.acknowledged:
                    print("[INFO] Inserted {} documents".format(len(doc)))
                else:
                    print("[ERROR] Could not insert {} documents".format(len(doc)))

    def get_pvp_summary(self, doc, region):
        namespace = "profile-{region}".format(region=region)
        if debug:
            self.logger("[DEBUG] get_pvp_summary({doc}, {namespace})".format(doc=doc, namespace=namespace))
        res = self.oauth.oauth_api_call(pvp_summary_url.format(region=region, realm=self.realm_slug[doc['realm']], character=doc['name'].lower(), namespace=namespace))
        if res.status_code == 200:
            stats_json = json.loads(res.text)
            if 'honor_level' in stats_json:
                doc.update({
                    'honor_level': stats_json['honor_level']
                })
            if 'brackets' in stats_json:
                for bracket in stats_json['brackets']:
                    res = self.oauth.oauth_api_call(bracket['href'])
                    if res.status_code == 200:
                        stats_bracket = json.loads(res.text)
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
                    else:
                        self.logger("[ERROR] Unexpected bracket error {region}-{realm}-{name}".format(region=region, realm=doc['realm'], name=doc['name']))
                        self.logger("[ERROR] [{code}] {text}".format(code=res.status_code, text=res.text))
                        return None

        elif res.status_code in [403, 404]:
            self.logger("[WARN] Characters {region}-{realm}-{name} not found".format(region=region, realm=doc['realm'], name=doc['name']))
            return False
        else:
            self.logger("[ERROR] Unexpected summary error for {region}-{realm}-{name}".format(region=region, realm=doc['realm'], name=doc['name']))
            self.logger("[ERROR] [{code}] {text}".format(code=res.status_code, text=res.text))
            return None
        return True

    def update_characters(self, region, faction):
        db_characters = self.mongo.db['pvpdb']['characters_{r}_{f}'.format(r=region, f=faction)]
        killer = GracefulKiller()
        while not killer.kill_now:
            doc = db_characters.find_one_and_update(
                {"lastModified": None},
                {"$currentDate": {"lastModified": True}}
            )
            if doc == None:
                self.logger("[INFO] No update found for {region} {faction}".format(region=region, faction=faction))
                break
            if doc['realm'] not in self.realm_slug:
                self.logger("[WARN] Realm not found for {region}-{faction}-{realm}-{name}".format(region=region, faction=faction, realm=doc['realm'], name=doc['name']))
                db_characters.remove({"_id": doc['_id']})
                continue
            updated = self.get_pvp_summary(doc, region)
            if updated == None:
                res = db_characters.update_one(
                    {"_id": doc['_id']},
                    {
                        "$set": {"lastModified": None}
                    }
                )
                if res.acknowledged:
                    self.logger("[WARN] Reset lastModified for {region}-{realm}-{name}".format(region=region, realm=doc['realm'], name=doc['name']))
                else:
                    self.logger("[ERROR] Mongo error for update {region}-{realm}-{name}".format(region=region, realm=doc['realm'], name=doc['name']))
            elif updated == True:
                del doc['lastModified']
                res = db_characters.update_one(
                    {"_id": doc['_id']},
                    {
                        "$set": doc,
                        "$currentDate": {"lastModified": True}
                    }
                )
                if res.acknowledged:
                    self.logger("[INFO] Updated {region}-{realm}-{name}".format(region=region, realm=doc['realm'], name=doc['name']), False)
                else:
                    self.logger("[ERROR] Mongo error for update {region}-{realm}-{name}".format(region=region, realm=doc['realm'], name=doc['name']))
            else:
                self.logger("[WARN] Deleting {region}-{realm}-{name}".format(region=region, realm=doc['realm'], name=doc['name']), False)
                db_characters.remove({"_id": doc['_id']})

        if killer.kill_now:
            self.logger("[INFO] Graceful shutdown...")
            sys.exit(0)

    def __init__(self):
        # Check usage
        if len(sys.argv) <= 1:
            self.logger("[ERROR] Usage: workers-pvpdb.py <tokenid> [action] [region] [faction]")
            sys.exit(1)

        self.realm_slug = self.generate_realm_slug('rio/db_realms.lua')
        self.oauth = Oauth()
        self.mongo = Mongo()

def main():
    worker = Worker()
    if len(sys.argv) >= 3 and sys.argv[2] == "init":
        for r in ["eu", "us", "kr", "tw"]:
            for f in ["alliance", "horde"]:
                worker.init_characters(r, f)
    elif len(sys.argv) >= 3 and sys.argv[2] == "update":
        for r in ["eu", "us", "kr", "tw"]:
            for f in ["alliance", "horde"]:
                worker.update_characters(r, f)

main()