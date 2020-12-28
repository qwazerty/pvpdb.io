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
season_url = "https://{region}.api.blizzard.com/data/wow/pvp-season/index?namespace={namespace}"
character_days_ttl = 7

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
    progress = {"current": 0, "total": 0, "timer": 0}
    region = None
    faction = None
    current_season = None

    def logger(self, msg, newline=True, showTimer=False):
        print(" "*os.get_terminal_size()[0], end='\r')
        if showTimer:
            print("[{current}/{total}]".format(current=self.progress['current'], total=self.progress['total']), end='')
        print(msg, end='\n' if newline else '\r')

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

    def set_current_season(self):
        season_namespace = 'dynamic-{region}'.format(region=self.region)
        res_season = self.oauth.oauth_api_call(season_url.format(region=self.region, namespace=season_namespace))
        self.current_season = json.loads(res_season.text)['current_season']['id']

    def get_characters_list(self, file):
        characters = {}
        with open(file, "r") as f:
            for line in f:
                if ("F = function()" in line):
                    r = re.split('"', line)[1]
                    c = re.split('{|}', line)[1].replace('"', '').split(",")[1:]
                    characters.update({r: c})
        return characters

    def init_characters(self):
        characters = self.get_characters_list("rio/db_{region}_{faction}_characters.lua".format(region=self.region, faction=self.faction))
        db_characters = self.mongo.db['pvpdb']['characters_{r}_{f}'.format(r=self.region, f=self.faction)]
        db_characters.create_index([('lastModified', pymongo.ASCENDING)])
        db_characters.create_index([('name', pymongo.ASCENDING)])
        db_characters.create_index([('realm', pymongo.ASCENDING)])
        db_characters.create_index([('name', pymongo.ASCENDING), ('realm', pymongo.ASCENDING)], unique=True)
        for realm in characters:
            self.logger("[INFO] Init {region}-{faction}-{realm}".format(region=self.region, faction=self.faction, realm=realm))
            doc = [ { "name": c, "realm": realm, "lastModified": None } for c in characters[realm] if db_characters.find_one({"name": c, "realm": realm}) is None and realm in self.realm_slug ]
            if len(doc) == 0:
                print("[INFO] 0 documents to insert, skipping")
            else:
                res = db_characters.insert_many(doc)
                if res.acknowledged:
                    print("[INFO] Inserted {} documents".format(len(doc)))
                else:
                    print("[ERROR] Could not insert {} documents".format(len(doc)))

    def get_pvp_summary(self, doc):
        namespace = "profile-{region}".format(region=self.region)
        if debug:
            self.logger("[DEBUG] get_pvp_summary({doc}, {namespace})".format(doc=doc, namespace=namespace))
        res = self.oauth.oauth_api_call(pvp_summary_url.format(region=self.region, realm=self.realm_slug[doc['realm']], character=doc['name'].lower(), namespace=namespace))
        if res.status_code == 200:
            try:
                stats_json = json.loads(res.text)
            except json.decoder.JSONDecodeError as e:
                self.logger("[ERROR] {e}".format(e=e.msg))
                self.logger("[DEBUG] {text}".format(text=res.text))
                return None
            if 'honor_level' in stats_json:
                doc.update({
                    'honor_level': stats_json['honor_level']
                })
            if 'brackets' in stats_json:
                for bracket in stats_json['brackets']:
                    res = self.oauth.oauth_api_call(bracket['href'].replace('http://', 'https://'))
                    if res.status_code == 200:
                        stats_bracket = json.loads(res.text)
                        if 'bracket' in stats_bracket:
                            doc.setdefault('pvp-bracket', {})
                            doc['pvp-bracket'].setdefault(stats_bracket['bracket']['type'], {})
                            doc['pvp-bracket'][stats_bracket['bracket']['type']].setdefault('current_statistics', {})
                            doc['pvp-bracket'][stats_bracket['bracket']['type']].setdefault('s{current_season}_statistics'.format(current_season=self.current_season), {})
                            statistics = {
                                'rating': stats_bracket['rating'],
                                'played': stats_bracket['season_match_statistics']['played'],
                                'won': stats_bracket['season_match_statistics']['won'],
                                'lost': stats_bracket['season_match_statistics']['lost']
                            }
                            doc['pvp-bracket'][stats_bracket['bracket']['type']]['current_statistics'].update(statistics)
                            doc['pvp-bracket'][stats_bracket['bracket']['type']]['s{current_season}_statistics'.format(current_season=self.current_season)].update(statistics)
                    elif res.status_code in [403, 404]:
                        self.logger("[WARN] Bracket {region}-{realm}-{name} not found".format(region=self.region, realm=doc['realm'], name=doc['name']), False)
                        return False
                    else:
                        self.logger("[ERROR] Unexpected bracket error {region}-{realm}-{name}".format(region=self.region, realm=doc['realm'], name=doc['name']))
                        self.logger("[ERROR] [{code}] {text}".format(code=res.status_code, text=res.text))
                        if res.status_code == 503:
                            self.logger("[ERROR] Sleep 600 seconds...")
                            time.sleep(600)
                        return None

        elif res.status_code in [403, 404]:
            self.logger("[WARN] Characters {region}-{realm}-{name} not found".format(region=self.region, realm=doc['realm'], name=doc['name']), False)
            return False
        else:
            self.logger("[ERROR] Unexpected summary error for {region}-{realm}-{name}".format(region=self.region, realm=doc['realm'], name=doc['name']))
            self.logger("[ERROR] [{code}] {text}".format(code=res.status_code, text=res.text))
            return None
        return True

    def update_characters(self):
        db_characters = self.mongo.db['pvpdb']['characters_{r}_{f}'.format(r=self.region, f=self.faction)]
        self.progress["total"] = db_characters.count({})
        self.progress['timer'] = 0
        killer = GracefulKiller()
        while not killer.kill_now:
            self.progress['timer'] -= 1
            if self.progress['timer'] <= 0:
                self.progress['timer'] = 10
                d = datetime.datetime.now() + datetime.timedelta(days=-character_days_ttl)
                self.progress["current"] = db_characters.count({"lastModified": { "$lte": d }})
            doc = db_characters.find_one_and_update(
                {"lastModified": { "$lte": d }},
                {"$currentDate": {"lastModified": True}}
            )
            if doc == None:
                self.logger("[INFO] No update found for {region} {faction}".format(region=self.region, faction=self.faction), newline=True, showTimer=True)
                break
            if doc['realm'] not in self.realm_slug:
                self.logger("[WARN] Realm not found for {region}-{faction}-{realm}-{name}".format(region=self.region, faction=self.faction, realm=doc['realm'], name=doc['name']))
                db_characters.remove({"_id": doc['_id']})
                continue
            updated = self.get_pvp_summary(doc)
            if updated == None:
                res = db_characters.update_one(
                    {"_id": doc['_id']},
                    {
                        "$set": {"lastModified": None}
                    }
                )
                if res.acknowledged:
                    self.logger("[WARN] Reset lastModified for {region}-{realm}-{name}".format(region=self.region, realm=doc['realm'], name=doc['name']), newline=False, showTimer=True)
                else:
                    self.logger("[ERROR] Mongo error for update {region}-{realm}-{name}".format(region=self.region, realm=doc['realm'], name=doc['name']))
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
                    self.logger("[INFO] Updated {region}-{realm}-{name}".format(region=self.region, realm=doc['realm'], name=doc['name']), newline=False, showTimer=True)
                else:
                    self.logger("[ERROR] Mongo error for update {region}-{realm}-{name}".format(region=self.region, realm=doc['realm'], name=doc['name']))
            else:
                self.logger("[WARN] Deleting {region}-{realm}-{name}".format(region=self.region, realm=doc['realm'], name=doc['name']), newline=False, showTimer=True)
                db_characters.remove({"_id": doc['_id']})

        if killer.kill_now:
            self.logger("[INFO] Graceful shutdown")
            sys.exit(0)

    def __init__(self, region, faction):
        # Check usage
        if len(sys.argv) <= 1:
            self.logger("[ERROR] Usage: workers-pvpdb.py <tokenid> [action] [region] [faction]")
            sys.exit(1)

        self.realm_slug = self.generate_realm_slug('rio/db_realms.lua')
        self.oauth = Oauth()
        self.mongo = Mongo()
        self.region = region
        self.faction = faction
        self.set_current_season()

def main():
    if len(sys.argv) >= 3 and sys.argv[2] == "init":
        for r in ["eu", "us", "kr", "tw"]:
            for f in ["alliance", "horde"]:
                worker = Worker(r, f)
                worker.init_characters()
    elif len(sys.argv) >= 3 and sys.argv[2] == "update":
        for r in ["eu", "us", "kr", "tw"]:
            for f in ["alliance", "horde"]:
                worker = Worker(r, f)
                worker.update_characters()

main()
