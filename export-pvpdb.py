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

addon_version = '1.0.3'
addon_path = "../PvPDB"

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
    with open('{path}/db/db_realms_{region}.lua'.format(path=addon_path, region=region), 'w') as f:
        f.write('local _, ns = ...\n')
        f.write('local region = "{region}"\n'.format(region=region))
        f.write('local F\n\n')
        for realm in realms:
            for faction in ["a", "h"]:
                f.write('F = function() ns.db{faction}["{realm}"]={{}} end; F()\n'.format(faction=faction[:1], realm=realm))

def export_characters(db_characters, region, faction):
    print("[INFO] Export {region}-{faction}".format(region=region, faction=faction))
    realms = db_characters.distinct('realm')
    with open('{path}/db/db_characters_{region}_{faction}.lua'.format(path=addon_path, region=region, faction=faction), 'w') as f:
        f.write('local _, ns = ...\n')
        f.write('local region = "{region}"\n'.format(region=region))
        f.write('local F\n\n')
        f.write('local function Load(self, event, ...)\n')
        for realm in realms:
            f.write('F = function() ns.db{faction}["{realm}"]={{'.format(faction=faction[:1], realm=realm))
            characters = db_characters.find({'realm': realm})
            i = 0
            for char in characters:
                j = 0
                #if 'honor_level' in char:
                #    if j == 0:
                #        f.write('["{name}"]={{'.format(name=char['name']))
                #    f.write('["hl"]={hl}'.format(hl=char['honor_level']))
                #    j += 1
                if 'pvp-bracket' in char:
                    for bracket_id, bracket_slug in ('ARENA_2v2', '2v2'), ('ARENA_3v3', '3v3'), ('BATTLEGROUNDS', 'bg'):
                        if bracket_id in char['pvp-bracket'] and 'current_statistics' in char['pvp-bracket'][bracket_id]:
                            if j == 0:
                                if i != 0:
                                    f.write(',')
                                i += 1
                                f.write('["{name}"]={{'.format(name=char['name']))
                            else:
                                f.write(',')
                            j += 1
                            char_bracket = char['pvp-bracket'][bracket_id]['current_statistics']
                            f.write('["{bracket_slug}"]={{{cr},{won},{lost}}}'.format(bracket_slug=bracket_slug,cr=char_bracket['rating'],won=char_bracket['won'], lost=char_bracket['lost']))
                if j != 0:
                    f.write('}')
            f.write('} end; F()\n')
        f.write('end\n')
        f.write('local Load_Frame = CreateFrame("FRAME")\n')
        f.write('if region == ns.REGION then\n')
        f.write('    Load_Frame:RegisterEvent("PLAYER_ENTERING_WORLD")\n')
        f.write('    Load_Frame:SetScript("OnEvent", Load)\n')
        f.write('end\n')

def update_toc():
    with open('{path}/PvPDB.toc'.format(path=addon_path), 'w') as f:
        f.write('## Interface: 90002\n')
        f.write('## Title: PvPDB\n')
        f.write('## Author: Qwazerty\n')
        f.write('## Version: 1.0.3-{date}\n'.format(date=datetime.datetime.today().strftime('%Y%m%d')))
        f.write('## Notes: Show PvP ranking information on tooltips\n\n')

        f.write('PvPDB.lua\n')
        for r in ["eu", "us", "kr", "tw"]:
            for faction in ["alliance", "horde"]:
                f.write('db/db_characters_{r}_{faction}.lua\n'.format(r=r, faction=faction))

def main():
    for r in ["eu", "us", "kr", "tw"]:
        for f in ["alliance", "horde"]:
            export_characters(pvpdb['characters_{r}_{f}'.format(r=r, f=f)], r, f)
    update_toc()

main()
