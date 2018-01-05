import base64
import concurrent.futures
import gzip
import itertools
import json
import os
import pickle
import requests
import urllib.request
from typing import Tuple

import pyrebase
import time

DISCORD_BOT_TOKEN = os.getenv("BOT_TOKEN", "")
GUILD_ID = os.getenv("GUILD_ID", "")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "")
ANNOUNCEMENT_CHANNEL_ID = os.getenv("ANNOUNCEMENT_CHANNEL_ID", "219804933916983296")
ALLIN_MEMBER_ROLE_ID = os.getenv("ALLIN_MEMBER_ROLE_ID", "")
ANNOUNCE_URL = os.getenv("ANNOUNCE_URL", "http://localhost:40862")
FIREBASE_CONFIG = os.getenv("FIREBASE_CONFIG", "")
WIN_STREAKS_CACHE_FILE = os.getenv("WIN_STREAKS_CACHE_FILE", "win_streaks.cache")

WIN_STREAK_MESSAGES = {
    4: "<@{}> is on a 4 game win streak!",
    6: "Killing spree! <@{}> is on a 6 game win streak!",
    8: "RAMPAGE. <@{}> is on an 8 game win streak!",
    9: "<@{}> is completely dominating with a 9 win streak!",
    10: "U N S T O P P A B L E. <@{}> is on a 10 win streak!",
    15: "🎉 🎉 🎉 🎉 Congratulations!  🎉 🎉 🎉 🎉\n<@{}> has gone 15 games without losing a single one!"
}
SECONDS_IN_5_DAYS = 432000

def fetch_win_streaks(member: str) -> Tuple[str, int]:
    db = create_db_connection()
    regions = db.child("members").child(member).child("characters").get().val()
    if not regions:
        regions = {}

    characters = itertools.chain.from_iterable(x.values() for x in regions.values())

    def map_characters_to_win_streaks(character: dict) -> int:
        ladder_info = character.get("ladder_info", {})
        sorted_seasons = list(sorted(ladder_info.keys(), reverse=True))
        if sorted_seasons:
            race_win_streaks = (
                x.get("current_win_streak", 0)
                for x
                in ladder_info[sorted_seasons[0]].values()
                if x.get("last_played_time_stamp", 0) > time.time() - SECONDS_IN_5_DAYS)
            return max(race_win_streaks, default=0)
        else:
            return 0

    character_win_streaks = list(map(map_characters_to_win_streaks, characters))
    return member, max(character_win_streaks, default=0)


def announce_win_streak(member: str, streak: int, previous_streak: int) -> None:

    if streak >= previous_streak and streak in WIN_STREAK_MESSAGES:
        data = {
            "channel_id": ANNOUNCEMENT_CHANNEL_ID,
            "message": WIN_STREAK_MESSAGES.get(streak).format(member)
        }

        stream_data = fetch_stream_data(member)
        if stream_data.get("name", "") and stream_data.get("type", "") == "live":
            stream_name = stream_data["name"]
            data["message"] += "\nTune in to https://www.twitch.tv/{} and show your support!".format(stream_name)
        try:
            urllib.request.urlopen(ANNOUNCE_URL, data=json.dumps(data).encode("utf-8"))
        except urllib.request.URLError as e:
            print("Error announcing member {} with streak {}".format(member, str(streak)))
            print(e)


def fetch_stream_data(member: str) -> dict:
    db = create_db_connection()
    twitch_connection = db.child("members").child(member).child("connections").child("twitch").get().val()
    if not twitch_connection or not twitch_connection.get("id", "") or not twitch_connection.get("name", ""):
        return {}

    url = "https://api.twitch.tv/helix/streams?first=1&user_id={}".format(twitch_connection["id"])
    request = urllib.request.Request(url, headers={"Client-ID": TWITCH_CLIENT_ID})
    try:
        response = urllib.request.urlopen(request)
        stream_data = json.loads(response.read().decode("utf-8"))

        if stream_data.get("data", []):
            result = stream_data["data"][0]
            result["name"] = twitch_connection["name"]
            return result

    except urllib.request.URLError as e:
        print(e)

    return {}


def main():
    db = create_db_connection()
    members = db.child("members").shallow().get().val()
    if not members:
        members = []

    url = "https://discordapp.com/api/guilds/{}/members?limit=500".format(GUILD_ID)
    response = requests.get(url, headers={'Authorization': 'Bot ' + DISCORD_BOT_TOKEN})
    guild_members = response.json() if response.status_code == 200 else []
    allin_members_lookup = set(
        x.get("user", {}).get("id", "")
        for x
        in guild_members
        if ALLIN_MEMBER_ROLE_ID in x.get("roles", []))

    members = [x for x in members if x in allin_members_lookup]

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures, _ = concurrent.futures.wait([executor.submit(fetch_win_streaks, member) for member in members])
        win_streaks = [future.result() for future in futures if future.done()]

    if os.path.exists(WIN_STREAKS_CACHE_FILE):
        with open(WIN_STREAKS_CACHE_FILE, "rb") as file:
            win_streaks_cache = dict(pickle.load(file))

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            concurrent.futures.wait([
                executor.submit(announce_win_streak, member, streak, win_streaks_cache.get(member, 0))
                for member, streak
                in win_streaks])

    with open(WIN_STREAKS_CACHE_FILE, "wb") as file:
        pickle.dump(win_streaks, file)


def create_db_connection():
    db_config = pickle.loads(gzip.decompress(base64.b64decode(FIREBASE_CONFIG)))
    db = pyrebase.initialize_app(db_config).database()
    return db


if __name__ == "__main__":
    main()