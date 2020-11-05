#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import json
import time
import datetime
import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from oauthlib.oauth2 import TokenExpiredError

try:
    import tokens
except ImportError:
    print("[ERROR] Create a credentials file tokens.py with:")
    print("[ERROR]   client_id = 'CLIENT_ID'")
    print("[ERROR]   client_secret = 'CLIENT_SECRET'")
    print("[ERROR]")
    print("[ERROR] To redeem IDs, check https://develop.battle.net/access/")
    sys.exit(1)

token_url = 'https://eu.battle.net/oauth/token'
client_id = tokens.client_id
client_secret = tokens.client_secret

client = BackendApplicationClient(client_id=client_id)
def oauth_login(client):
    oauth = OAuth2Session(client=client)
    return oauth.fetch_token(token_url=token_url, client_id=client_id, client_secret=client_secret)

raiderio_path = ""
pvp_summary_url = "https://eu.api.blizzard.com/profile/wow/character/{realm}/{character}/pvp-summary?namespace={namespace}"
pvp_bracket_url = "https://eu.api.blizzard.com/profile/wow/character/{realm}/{character}/pvp-bracket/{bracket}?namespace={namespace}"
token = oauth_login(client)
character_days_ttl = 90

db = {}

def oauth_api_call(url):
    global token
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
        print("[WARN] > ConnectionError. Retrying in 60s...")
        token = oauth_login(client)
        return oauth_api_call(url)

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

def get_pvp_summary(db, namespace, realm, character, realm_slug):
    stats_summary = oauth_api_call(pvp_summary_url.format(realm=realm_slug[realm], character=character.lower(), namespace=namespace))
    if 'honor_level' in stats_summary:
        print("[INFO] > Get honor_level for '{realm}-{character}'".format(realm=realm, character=character))
        db[realm]['characters'][character].update({
            'honor_level': stats_summary['honor_level']
        })
    else:
        print("[INFO] > Could not find honor_level for '{realm}-{character}'".format(realm=realm, character=character))
    if 'brackets' in stats_summary:
        for bracket in stats_summary['brackets']:
            stats_bracket = oauth_api_call(bracket['href'])
            if 'bracket' in stats_bracket:
                print("[INFO] > Get {bracket_type} for '{realm}-{character}'".format(bracket_type=stats_bracket['bracket']['type'], realm=realm, character=character))
                db[realm]['characters'][character].update({
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
            print("[INFO] > Could not find any bracket for '{realm}-{character}'".format(realm=realm, character=character))

def foreach_characters(faction, realm_slug):
    characters = get_characters_list(raiderio_path + "db_eu_{faction}_characters.lua".format(faction=faction))
    try:
        with open("db_eu_{faction}.json".format(faction=faction), "r", encoding='utf8') as f:
            db = json.loads(f.read())
    except json.decoder.JSONDecodeError:
        print("[WARN] Empty 'db_eu_{faction}.json' file".format(faction=faction))
        db = {}
    except FileNotFoundError:
        print("[WARN] File 'db_eu_{faction}.json' not found".format(faction=faction))
        db = {}
    for realm in characters:
        if realm in db:
            print("[INFO] Opening realm '{realm}'".format(realm=realm))
        else:
            print("[INFO] Creating realm '{realm}'".format(realm=realm))
            db[realm] = {
                "characters": {}
            }
        for c in characters[realm]:
            if c in db[realm]['characters'] and datetime.datetime.now() < datetime.datetime.strptime(db[realm]['characters'][c]['updated'], '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(days=character_days_ttl):
                print("[INFO] > Not updating '{realm}-{character}'".format(realm=realm, character=c))
            else:
                db[realm]['characters'][c] = {
                    "updated": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                }
                get_pvp_summary(db, "profile-eu", realm, c, realm_slug)
                with open("db_eu_{faction}.json.tmp".format(faction=faction), "w", encoding='utf8') as f:
                    f.write(json.dumps(db, indent=2, ensure_ascii=False))
                    f.flush()
                    os.fsync(f.fileno())
                os.rename("db_eu_{faction}.json.tmp".format(faction=faction), "db_eu_{faction}.json".format(faction=faction))

def update_characters_summary(realm_slug):
    # Alliance
    foreach_characters("alliance", realm_slug)
    # Horde
    foreach_characters("horde", realm_slug)

def main():
    realm_slug = generate_realm_slug(raiderio_path + "db_realms.lua")
    update_characters_summary(realm_slug)

main()
