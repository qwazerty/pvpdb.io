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

pvp_summary_url = "https://eu.api.blizzard.com/profile/wow/character/{realm}/{character}/pvp-summary?namespace={namespace}"
pvp_bracket_url = "https://eu.api.blizzard.com/profile/wow/character/{realm}/{character}/pvp-bracket/{bracket}?namespace={namespace}"
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
        if (res.status_code == 200):
            return json.loads(res.text)
        else:
            return {}
    except requests.exceptions.ConnectionError:
        print("[WARN] > ConnectionError. Skipping...")
        print("[WARN] > {}".format(url))

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

def get_characters_list(file):
    characters = {}
    with open(file, "r") as f:
        for line in f:
            if ("F = function()" in line):
                r = re.split('"', line)[1]
                c = re.split('{|}', line)[1].replace('"', '').split(",")[1:]
                characters.update({r: c})
    return characters

def init_characters(db_characters, region, faction, realm_slug):
    characters = get_characters_list("db_{region}_{faction}_characters.lua".format(region=region, faction=faction))
    db_characters.create_index([('lastModified', pymongo.ASCENDING)])
    db_characters.create_index([('name', pymongo.ASCENDING)])
    db_characters.create_index([('realm', pymongo.ASCENDING)])
    db_characters.create_index([('name', pymongo.ASCENDING), ('realm', pymongo.ASCENDING)], unique=True)
    for realm in characters:
        print("[INFO] Init realm {region}-{realm}".format(region=region, realm=realm))
        doc = [ { "name": c, "realm": realm, "lastModified": None } for c in characters[realm] if db_characters.find_one({"name": c, "realm": realm}) is None ]
        if len(doc) == 0:
            print("[INFO] 0 documents to insert, skipping")
        else:
            res = db_characters.insert_many(doc)
            if res.acknowledged:
                print("[INFO] Inserted {} documents".format(len(doc)))
            else:
                print("[ERROR] Could not insert {} documents".format(len(doc)))

def get_pvp_summary(doc, namespace):
    if debug:
        print("[DEBUG] get_pvp_summary({doc}, {namespace})".format(doc=doc, namespace=namespace))
    stats_summary = oauth_api_call(pvp_summary_url.format(realm=realm_slug[doc['realm']], character=doc['name'].lower(), namespace=namespace))
    if 'honor_level' in stats_summary:
        doc.update({
            'honor_level': stats_summary['honor_level']
        })
    if 'brackets' in stats_summary:
        for bracket in stats_summary['brackets']:
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
        get_pvp_summary(doc, "profile-{region}".format(region=region))
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
            print("[ERROR] Could not update {}".format(doc))
    if killer.kill_now:
        print("[INFO] Graceful shutdown...")
        sys.exit(0)

def main():
    if len(sys.argv) >= 3 and sys.argv[2] == "init":
        if len(sys.argv) == 3 or sys.argv[3] == "alliance":
            print("[INFO] Init Alliance")
            init_characters(pvpdb['characters_eu_alliance'], "eu", "alliance", realm_slug)
        if len(sys.argv) == 3 or sys.argv[3] == "horde":
            print("[INFO] Init Horde")
            init_characters(pvpdb['characters_eu_horde'], "eu", "horde", realm_slug)
        if len(sys.argv) >= 4 and sys.argv[3] == "test":
            print("[INFO] Init Test")
            init_characters(pvpdb['characters_eu_test'], "eu", "test", realm_slug)
    elif len(sys.argv) >= 3 and sys.argv[2] == "update":
        if len(sys.argv) == 3 or sys.argv[3] == "alliance":
            print("[INFO] Update Alliance")
            update_characters(pvpdb['characters_eu_alliance'], "eu", "alliance")
        if len(sys.argv) == 3 or sys.argv[3] == "horde":
            print("[INFO] Update Horde")
            update_characters(pvpdb['characters_eu_horde'], "eu", "horde")
        if len(sys.argv) >= 4 and sys.argv[3] == "test":
            print("[INFO] Update Test")
            update_characters(pvpdb['characters_eu_test'], "eu", "test")

realm_slug = generate_realm_slug("db_realms.lua")
main()

#c = "Aarista"
#realm = "Durotan"
#doc = pvpdb['characters_eu_alliance'].find_one_and_update({"name": c, "realm": realm}, {"$currentDate": {"lastModified": True}})
#doc = pvpdb['characters_eu_alliance'].find_one_and_update(
#    {"name": c, "realm": realm},
#    {
#        "$set": {"honor_level": 5},
#        "$currentDate": {"lastModified": True}
#    }
#)
#print(doc)
