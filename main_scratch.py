
import requests, json, sqlite3, psycopg2, os
import pandas as pd
import tkinter as tk
from url_params import *
from tkinter import ttk
from pandas import json_normalize
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

API_KEY = os.getenv("API_KEY")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PW")

headers = {
    "accept": "application/json",
    "x-api-key": API_KEY
}

connection = psycopg2.connect(
    host="localhost",
    database="nhl_db",
    user="postgres",
    password=PASSWORD,
    port="5432"
)

cursor = connection.cursor()

type_map = {
    "object": "TEXT",
    "int64": "INT",
    "float64": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP"
}


def gen_team_dict():
    cursor.execute(f"SELECT name, team_id from init.team_id;")
    team_list = cursor.fetchall()
    sampdict = {}
    for i in team_list:
        sampdict[i[0]] = i[1]
    return sampdict

def update_team_id():
    teams = {}
    id_upload = []
    for i in data:
        for j in i['divisions']:
            for z in j['teams']:
                teams[z['name']] = z['id']
    ## PUSHES DICT TO IN MEMORY DB
    for key, value in teams.items():
        id_upload.append((key, value))
    cursor.executemany("INSERT INTO init.team_id(name, team_id) VALUES(%s, %s)", id_upload)
    connection.commit()
    return teams

def get_shot_info():
    # selected_team = combo.get()
    # root.destroy()
    # cursor.execute(f"SELECT team_id from init.team_id WHERE name = '{selected_team}';")
    cursor.execute(f"SELECT team_id from init.team_id WHERE name = 'Devils';")
    team = cursor.fetchone()
    url = f"https://api.sportradar.com/nhl/trial/v7/en/seasons/2025/REG/teams/{team[0]}/analytics.json"
    response = requests.get(url, headers=headers)

    ## Creating AVG dataset and posting to DB
    avg_data = json_normalize(response.json()['own_record']['statistics']['average'])
    avg_data = avg_data.drop(avg_data.filter(regex="^shots.").columns, axis=1)
    avg_data['team'] = 'Devils'
    avg_data['line_type'] = 'Devils_avg'
    # avg_data['team'] = selected_team
    # avg_data['line_type'] = f"{selected_team}_avg"

    ## Creating TOTALS dataset and posting to DB
    tot_data = json_normalize(response.json()['own_record']['statistics']['total'])
    tot_data = tot_data.drop(tot_data.filter(regex="^shots.").columns, axis=1)
    tot_data['team'] = 'Devils'
    tot_data['line_type'] = 'Devils_total'
    # tot_data['team'] = selected_team
    # tot_data['line_type'] = f"{selected_team}_total"
    return avg_data, tot_data

def get_teamid(team):
    response = cursor.execute(f"SELECT team_id from init.team_id WHERE name = '{team}';")
    team_id = cursor.fetchone()[0]
    return team_id

def post_shot_info(get_shot_info):
    avg_data = get_shot_info()[0]
    tot_data = get_shot_info()[1]
    columns = []
    ## AVG Post
    for col, dtype in avg_data.dtypes.items():
        pg_type = type_map.get(str(dtype), "TEXT")
        if col == "line_type":  # make name unique
            columns.append(f"{col} {pg_type} UNIQUE")
        else:
            columns.append(f"{col} {pg_type}")

    create_table_sql = f"CREATE TABLE IF NOT EXISTS init.shot_data ({', '.join(columns)});"
    cursor.execute(create_table_sql)
    connection.commit()

def pull_team_stats(team):
    team_id = get_teamid(team)
    url = f"https://api.sportradar.com/nhl/{access_level}/v7/{language_code}/seasons/{season_year}/{season_type}/teams/{team_id}/statistics.{format}"
    response = requests.get(url, headers=headers)
    data = response.json()['own_record']['statistics']['total']
    df = pd.DataFrame([data])
    df_filtered = df[['shots', 'goals', 'games_played', 'hits']]
    df_formatted = df_filtered.rename(columns={
        'shots': 'shot_total',
        'goals': 'goals_total',
        'games_played': 'games_total',
        'hits': 'hits_total'
        })
    df_formatted['name'] = team
    df_formatted['team_id'] = team_id
    df_formatted['wins_total'] = response.json()['own_record']['goaltending']['total']['wins']
    df_formatted['loss_total'] = response.json()['own_record']['goaltending']['total']['losses']
    df_formatted['saves_total'] = response.json()['own_record']['goaltending']['total']['saves']
    df_formatted['powerplay_perc'] = response.json()['own_record']['statistics']['powerplay']['percentage']
    df_formatted['penkill_perc'] = response.json()['own_record']['statistics']['shorthanded']['kill_pct']
    df_formatted['save_perc'] = response.json()['own_record']['goaltending']['total']['saves_pct']
    df_formatted['goals_allowed_total'] = response.json()['own_record']['goaltending']['total']['goals_against']
    return df_formatted

def get_next_opponent(api_key, after_date):
    # Step 1: Fetch schedule for date onward (you may choose multiple dates or full season)
    # Here we'll fetch the daily schedule for each date starting from `after_date`
    date_str = after_date.strftime("%Y-%m-%d")
    url = f"https://api.sportradar.com/nhl/trial/v7/en/games/{date_str}/schedule.json?api_key={api_key}"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    
    # Step 2: Filter games where the team participates and the scheduled date > after_date
    games = data.get("games", [])
    future_games = []
    for game in games:
        # parse scheduled time
        sched = datetime.fromisoformat(game["scheduled"].replace("Z", "+00:00"))
        if sched <= after_date:
            continue
        # check teams
        for competitor in game["home", "away"] if “home” in game else game.get("competitors", []):  # adjust depending on schema
            pass
        # better schema inspection (look at “home” and “away” keys)
        if game["home"]["id"] == team_sr_id or game["away"]["id"] == team_sr_id:
            future_games.append(game)
    
    if not future_games:
        return None  # No future games found
    
    # Step 3: Sort by scheduled date/time ascending
    future_games.sort(key=lambda g: datetime.fromisoformat(g["scheduled"].replace("Z", "+00:00")))
    next_game = future_games[0]
    
    # Step 4: Determine opponent
    if next_game["home"]["id"] == team_sr_id:
        opponent = next_game["away"]
        home_or_away = "home"
    else:
        opponent = next_game["home"]
        home_or_away = "away"
    
    return {
        "scheduled": next_game["scheduled"],
        "opponent_id": opponent["id"],
        "opponent_name": opponent["name"],
        "home_or_away": home_or_away,
        "game_id": next_game["id"]
    }

def update_team_stats(df: pd.DataFrame, table_name: str, engine_url: str):
    from sqlalchemy import create_engine
    engine = create_engine(engine_url)


# with connection as conn:

print(post_team_stats('Devils'))


# options_dict = gen_team_dict()

# root = tk.Tk()
# root.title("Select an Option")
# root.geometry("300x150")

# tk.Label(root, text="Choose an option:").pack(pady=10)

# combo = ttk.Combobox(root, values=list(options_dict.keys()), state="readonly")
# combo.pack()
# combo.current(0)

# submit_button = ttk.Button(root, text="Submit", command=post_shot_info(get_shot_info))
# submit_button.pack(pady=10)

# root.mainloop()