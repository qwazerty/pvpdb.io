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
from pymongo import MongoClient
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from oauthlib.oauth2 import TokenExpiredError

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

debug = False

client = pymongo.MongoClient(tokens.mongo_url)
pvpdb = client['pvpdb']

def export_realms(db_characters, region):
    print("[INFO] Export realms {region}".format(region=region))
    realms = db_characters.distinct('realm')
    with open('db/db_realms_{region}.lua'.format(region=region), 'w') as f:
        f.write('local _, ns = ...\n')
        f.write('local region = "{region}"\n'.format(region=region))
        f.write('local F\n\n')
        for realm in realms:
            for faction in ["a", "h"]:
                f.write('F = function() ns.db{faction}["{realm}"]={{}} end; F()\n'.format(faction=faction[:1], realm=realm))

def export_characters(db_characters, region, faction):
    print("[INFO] Export {region}-{faction}".format(region=region, faction=faction))
    realms = db_characters.distinct('realm')
    with open('db/db_characters_{region}_{faction}.lua'.format(region=region, faction=faction), 'w') as f:
        f.write('local _, ns = ...\n')
        f.write('local region = "{region}"\n'.format(region=region))
        f.write('local F\n\n')
        f.write('local function Load(self, event, ...)\n')
        for realm in realms:
            f.write('F = function() ns.db{faction}["{realm}"]={{'.format(faction=faction[:1], realm=realm))
            characters = db_characters.find({'realm': realm})
            for i,char in enumerate(characters):
                if i != 0:
                    f.write(',')
                f.write('["{name}"]={{'.format(name=char['name']))
                j = 0
                if 'honor_level' in char:
                    f.write('["hl"]={hl}'.format(hl=char['honor_level']))
                    j += 1
                if 'pvp-bracket' in char:
                    for bracket_id, bracket_slug in ('ARENA_2v2', '2v2'), ('ARENA_3v3', '3v3'), ('BATTLEGROUNDS', 'bg'):
                        if bracket_id in char['pvp-bracket']:
                            if j != 0:
                                f.write(',')
                            j += 1
                            char_bracket = char['pvp-bracket'][bracket_id]
                            f.write('["{bracket_slug}"]={{'.format(bracket_slug=bracket_slug))
                            f.write('["cr"]={cr},'.format(cr=char_bracket['current_rating']))
                            f.write('["sms"]={{{won},{lost}}}}}'.format(won=char_bracket['season_match_statistics']['won'], lost=char_bracket['season_match_statistics']['lost']))
                f.write('}')
            f.write('} end; F()\n')
        f.write('end\n')
        f.write('local Load_Frame = CreateFrame("FRAME")\n')
        f.write('if region == ns.REGION then\n')
        f.write('    Load_Frame:RegisterEvent("PLAYER_ENTERING_WORLD")\n')
        f.write('    Load_Frame:SetScript("OnEvent", Load)\n')
        f.write('end\n')

def main():
    for r in ["eu", "us", "kr", "tw"]:
        export_realms(pvpdb['characters_{r}_alliance'.format(r=r)], r)
        for f in ["alliance", "horde"]:
            export_characters(pvpdb['characters_{r}_{f}'.format(r=r, f=f)], r, f)

main()
