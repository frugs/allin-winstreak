import asyncio
import concurrent.futures
import itertools
import json
import os
import threading
import time
import urllib.request
from typing import Tuple, List, Dict

import pyrebase
import requests
import discord
import flask

if os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "") or os.getenv("GAE_INSTANCE", ""):
    from google.cloud import datastore

    def retrieve_config_value(key: str) -> str:
        datastore_client = datastore.Client()
        return datastore_client.get(datastore_client.key("Config", key))["value"]

    DISCORD_BOT_TOKEN = retrieve_config_value("discordBotToken")
    GUILD_ID = retrieve_config_value("discordGuildId")
    TWITCH_CLIENT_ID = retrieve_config_value("twitchClientId")
    ANNOUNCEMENT_CHANNEL_ID = int(retrieve_config_value("discordAnnouncementChannelId"))
    MEMBER_ROLE_ID = retrieve_config_value("discordMemberRoleId")
    FIREBASE_CONFIG = json.loads(retrieve_config_value("firebaseConfig"))

else:
    DISCORD_BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    GUILD_ID = os.getenv("GUILD_ID", "")
    TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "")
    ANNOUNCEMENT_CHANNEL_ID = int(os.getenv("ANNOUNCEMENT_CHANNEL_ID", "0"))
    MEMBER_ROLE_ID = os.getenv("MEMBER_ROLE_ID", "")
    FIREBASE_CONFIG = json.loads(os.getenv("FIREBASE_CONFIG", "{}"))

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

previous_win_streaks = {}


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


def send_discord_message(message: str):
    if not message:
        return

    def inner():
        event_loop = asyncio.new_event_loop()
        discord_client = discord.Client(loop=event_loop)

        @discord_client.event
        async def on_ready():
            announcement_channel = discord_client.get_channel(ANNOUNCEMENT_CHANNEL_ID)
            await announcement_channel.send(message)
            await discord_client.logout()

        event_loop.run_until_complete(discord_client.start(DISCORD_BOT_TOKEN))
        event_loop.close()

    worker = threading.Thread(target=inner)
    worker.start()
    worker.join()


def create_announcement_message_for_member(
        member_id: str,
        member_name: str,
        streak: int,
        previous_streak: int,
) -> str:

    if streak > previous_streak and streak in WIN_STREAK_MESSAGES:
        message = WIN_STREAK_MESSAGES.get(streak).format(member_name)

        stream_data = fetch_stream_data(member_id)
        if stream_data.get("name", "") and stream_data.get("type", "") == "live":
            stream_name = stream_data["name"]
            message += "\nTune in to https://www.twitch.tv/{} and show your support!".format(
                stream_name)

        return message


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
    return dict((
        x.get("user", {}).get("id", ""),
        x.get("nick", "") or x.get("user", {}).get("username", "")
        or x.get("user", {}).get("id", ""),
    ) for x in guild_members if MEMBER_ROLE_ID in x.get("roles", []))


def create_db_connection():
    db = pyrebase.initialize_app(FIREBASE_CONFIG).database()
    return db


def fetch_registered_users() -> List[str]:
    db = create_db_connection()
    registered_users = db.child("members").shallow().get().val()
    return registered_users or []


def create_win_streak_announcement(members_lookup: Dict[str, str],
                                   win_streaks: List[Tuple[str, int]]) -> str:
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures, _ = concurrent.futures.wait([
            executor.submit(
                create_announcement_message_for_member,
                member_id,
                members_lookup[member_id],
                streak,
                previous_win_streaks.get(member_id, 0),
            ) for member_id, streak in win_streaks
        ])
        return "\n".join(future.result() for future in futures if future.done() and future.result())


def check_for_win_streaks_and_announce():
    global previous_win_streaks

    registered_users = fetch_registered_users()
    members_lookup = create_members_lookup()
    registered_members = [x for x in registered_users if x in members_lookup]
    win_streaks = fetch_all_win_streaks(registered_members)

    # Don't announce win streaks if we haven't fetched them at least once
    if previous_win_streaks:
        announcement_message = create_win_streak_announcement(members_lookup, win_streaks)
        send_discord_message(announcement_message)

    previous_win_streaks = dict(win_streaks)


app = flask.Flask(__name__)
request_handler_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


@app.route("/update")
def update():
    if flask.request.headers.get("X-Appengine-Cron", "false") == "true":
        future = request_handler_executor.submit(check_for_win_streaks_and_announce)
        future.result(timeout=300)
        return "", 200
    else:
        return "Forbidden", 403


def main():
    app.run(host="localhost", port=21726, debug=True)


if __name__ == "__main__":
    main()
