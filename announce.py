import base64
import concurrent.futures
import gzip
import itertools
import json
import os
import pickle
import urllib.request
from typing import Tuple

import pyrebase
from pyrebase.pyrebase import Database

ANNOUNCEMENT_CHANNEL_ID = os.getenv("ANNOUNCEMENT_CHANNEL_ID", "219804933916983296")
ANNOUNCE_URL = os.getenv("ANNOUNCE_URL", "http://localhost:40862")
FIREBASE_CONFIG = os.getenv("FIREBASE_CONFIG", "")
WIN_STREAKS_CACHE_FILE = os.getenv("WIN_STREAKS_CACHE_FILE", "win_streaks.cache")
WIN_STREAK_MESSAGES = {
    4: "<@{}> is on a 4 win streak!",
    6: "Killing spree! <@{}> is on a 6 win streak!",
    8: "RAMPAGE. <@{}> is on an 8 win streak!",
    9: "<@{}> is completely dominating with a 9 win streak!",
    10: "U N S T O P P A B L E. <@{}> is on a 10 win streak!",
    15: "🎉 🎉 🎉 🎉 Congratulations! <@{}> has gone 15 games without losing a single one! 🎉 🎉 🎉 🎉"
}


def fetch_win_streaks(db: Database, member: str) -> Tuple[str, int]:
    regions = db.child("members").child(member).child("characters").get().val()
    if not regions:
        regions = {}

    characters = itertools.chain.from_iterable(x.values() for x in regions.values())

    def map_characters_to_win_streaks(character: dict) -> int:
        ladder_info = character.get("ladder_info", {})
        sorted_seasons = list(sorted(ladder_info.keys(), reverse=True))
        if sorted_seasons:
            race_win_streaks = (x.get("current_win_streak", 0) for x in ladder_info[sorted_seasons[0]].values())
            return max(race_win_streaks, default=0)
        else:
            return 0

    character_win_streaks = list(map(map_characters_to_win_streaks, characters))
    return member, max(character_win_streaks, default=0)


def announce_win_streak(member: str, streak: int) -> None:
    if streak in WIN_STREAK_MESSAGES:
        data = {
            "channel_id": ANNOUNCEMENT_CHANNEL_ID,
            "message": WIN_STREAK_MESSAGES.get(streak).format(member)
        }
        try:
            urllib.request.urlopen(ANNOUNCE_URL, data=json.dumps(data).encode("utf-8"))
        except urllib.request.URLError as e:
            print("Error announcing member {} with streak {}".format(member, str(streak)))
            print(e)


def main():
    db_config = pickle.loads(gzip.decompress(base64.b64decode(FIREBASE_CONFIG)))
    db = pyrebase.initialize_app(db_config).database()

    members = db.child("members").shallow().get().val()
    if not members:
        members = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures, _ = concurrent.futures.wait([executor.submit(fetch_win_streaks, db, member) for member in members])
        win_streaks = [future.result() for future in futures if future.done()]

    if os.path.exists(WIN_STREAKS_CACHE_FILE):
        with open(WIN_STREAKS_CACHE_FILE, "rb") as file:
            win_streaks_cache = pickle.load(file)

        for member, streak in win_streaks:
            if streak > win_streaks_cache.get(member, 0):
                announce_win_streak(member, streak)

    with open(WIN_STREAKS_CACHE_FILE, "wb") as file:
        pickle.dump(win_streaks, file)


if __name__ == "__main__":
    main()