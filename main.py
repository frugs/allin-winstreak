import base64
import concurrent.futures
import gzip
import itertools
import json
import os
import pickle
import requests
import urllib.request
from typing import Tuple, List, Dict

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
DEBUG = os.getenv("DEBUG", "false").casefold() == "true".casefold()

if DEBUG:

    def print_debug(msg):
        print(msg)
else:

    def print_debug(_):
        pass


WIN_STREAK_MESSAGES = {
    4: "{} is on a 4 game win streak!",
    6: "Killing spree! {} is on a 6 game win streak!",
    8: "RAMPAGE. {} is on an 8 game win streak!",
    9: "{} is completely dominating with a 9 win streak!",
    10: "U N S T O P P A B L E. {} is on a 10 win streak!",
    15: "🎉 🎉 🎉 🎉 Congratulations!  🎉 🎉 🎉 🎉\n"
        "{} has gone 15 games without losing a single one!",
}
SECONDS_IN_5_DAYS = 432000
WORKERS = 16


def fetch_win_streaks_for_member(member: str) -> Tuple[str, int]:
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
                x.get("current_win_streak", 0) for x in ladder_info[sorted_seasons[0]].values()
                if x.get("last_played_time_stamp", 0) > time.time() - SECONDS_IN_5_DAYS)
            return max(race_win_streaks, default=0)
        else:
            return 0

    character_win_streaks = list(map(map_characters_to_win_streaks, characters))
    return member, max(character_win_streaks, default=0)


def announce_win_streak(member_id: str, member_name: str, streak: int,
                        previous_streak: int) -> None:

    if streak > previous_streak and streak in WIN_STREAK_MESSAGES:
        print_debug("Previous streak: {}, Streak: {}".format(previous_streak, streak))
        print_debug("Announcing winstreak for member: ({}, {})".format(member_id, member_name))

        data = {
            "channel_id": ANNOUNCEMENT_CHANNEL_ID,
            "message": WIN_STREAK_MESSAGES.get(streak).format(member_name)
        }

        stream_data = fetch_stream_data(member_id)
        if stream_data.get("name", "") and stream_data.get("type", "") == "live":
            stream_name = stream_data["name"]
            data[
                "message"] += "\nTune in to https://www.twitch.tv/{} and show your support!".format(
                    stream_name)
        try:
            urllib.request.urlopen(ANNOUNCE_URL, data=json.dumps(data).encode("utf-8"))
        except urllib.request.URLError as e:
            print("Error announcing member ({}, {}) with streak {}".format(
                member_id, member_name, str(streak)))
            print(e)


def fetch_stream_data(member: str) -> dict:
    db = create_db_connection()
    twitch_connection = db.child("members").child(member).child("connections").child(
        "twitch").get().val()
    if not twitch_connection or not twitch_connection.get("id", "") or not twitch_connection.get(
            "name", ""):
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


def fetch_all_win_streaks(members: List[str]) -> List[Tuple[str, int]]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures, _ = concurrent.futures.wait(
            [executor.submit(fetch_win_streaks_for_member, member) for member in members])
        return [future.result() for future in futures if future.done()]


def create_members_lookup() -> Dict[str, str]:
    url = "https://discordapp.com/api/guilds/{}/members?limit=500".format(GUILD_ID)
    response = requests.get(url, headers={'Authorization': 'Bot ' + DISCORD_BOT_TOKEN})
    guild_members = response.json() if response.status_code == 200 else []
    return dict((x.get("user", {}).get("id", ""),
                 x.get("nick",
                       x.get("user", {}).get("username", ""))) for x in guild_members
                if ALLIN_MEMBER_ROLE_ID in x.get("roles", []))


def create_db_connection():
    db_config = pickle.loads(gzip.decompress(base64.b64decode(FIREBASE_CONFIG)))
    db = pyrebase.initialize_app(db_config).database()
    return db


def fetch_registered_users() -> List[str]:
    db = create_db_connection()
    registered_users = db.child("members").shallow().get().val()
    return registered_users or []


def load_previous_win_streaks() -> Dict[str, int]:
    if os.path.exists(WIN_STREAKS_CACHE_FILE):
        with open(WIN_STREAKS_CACHE_FILE, "rb") as file:
            return dict(pickle.load(file))


def save_win_streaks(win_streaks: List[Tuple[str, int]]):
    with open(WIN_STREAKS_CACHE_FILE, "wb") as file:
        pickle.dump(win_streaks, file)


def announce_all_win_streaks(
        members_lookup: Dict[str, str],
        previous_win_streaks: Dict[str, int],
        win_streaks: List[Tuple[str, int]],
):
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        concurrent.futures.wait([
            executor.submit(announce_win_streak, member_id, members_lookup[member_id], streak,
                            previous_win_streaks.get(member_id, 0))
            for member_id, streak in win_streaks
        ])


def main():
    registered_users = fetch_registered_users()
    members_lookup = create_members_lookup()
    registered_members = [x for x in registered_users if x in members_lookup]
    win_streaks = fetch_all_win_streaks(registered_members)
    previous_win_streaks = load_previous_win_streaks()
    announce_all_win_streaks(members_lookup, previous_win_streaks, win_streaks)
    save_win_streaks(win_streaks)


if __name__ == "__main__":
    main()
