import requests, json, sqlite3, psycopg2, os
import pandas as pd
import tkinter as tk
from url_params import *
from tkinter import ttk
from pandas import json_normalize
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
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


def get_teamid(team):
    response = cursor.execute(f"SELECT team_id from init.team_id WHERE name = '{team}';")
    team_id = cursor.fetchone()[0]
    return team_id


def post_team_id():
    teams = {}
    url = "https://api.sportradar.com/nhl/trial/v7/en/league/hierarchy.json"
    response = requests.get(url, headers=headers)
    data = response.json()['conferences']
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


def get_team_info(team_name):
    cursor.execute(f"SELECT team_id from init.team_id WHERE name = '{team_name}';")
    team = cursor.fetchone()
    url = f"https://api.sportradar.com/nhl/trial/v7/en/seasons/2025/REG/teams/{team[0]}/analytics.json"
    response = requests.get(url, headers=headers)
    avg_data = json_normalize(response.json()['own_record']['statistics']['average'])
    tot_data = json_normalize(response.json()['own_record']['statistics']['total'])
    return avg_data, tot_data


def get_shot_info(team):
    team_id = get_teamid(team)
    url = f"https://api.sportradar.com/nhl/trial/v7/en/seasons/2025/REG/teams/{team_id}/analytics.json"
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


def get_team_stats(team):
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


def update_team_stats(df: pd.DataFrame, table_name: str):
    engine_url = f"postgresql+psycopg2://postgres:{PASSWORD}@localhost:5432/nhl_db"
    engine = create_engine(engine_url)
    upsert_query = text(f"""
        INSERT INTO {table_name} (name, team_id, shot_total, saves_total, goals_total, 
            powerplay_perc, save_perc, penkill_perc, wins_total, loss_total, games_total, hits_total, goals_allowed_total)
        VALUES (:name, :team_id, :shot_total, :saves_total, :goals_total, 
            :powerplay_perc, :save_perc, :penkill_perc, :wins_total, :loss_total, :games_total, :hits_total, :goals_allowed_total)
        ON CONFLICT (name)
        DO UPDATE SET
            shot_total = EXCLUDED.shot_total,
            saves_total = EXCLUDED.saves_total,
            goals_total = EXCLUDED.goals_total,
            powerplay_perc = EXCLUDED.powerplay_perc,
            save_perc = EXCLUDED.save_perc,
            penkill_perc = EXCLUDED.penkill_perc,
            wins_total = EXCLUDED.wins_total,
            loss_total = EXCLUDED.loss_total,
            games_total = EXCLUDED.games_total,
            hits_total = EXCLUDED.hits_total,
            goals_allowed_total = EXCLUDED.goals_allowed_total;
    """)
    with engine.begin() as conn:
        conn.execute(upsert_query, df.to_dict(orient='records'))


def post_season_schedule(year):
    url = f"https://api.sportradar.com/nhl/{access_level}/v7/{language_code}/games/{year}/{season_type}/schedule.{format}"
    name_fix = {
    "Leafs": "Maple Leafs",
    "Wings": "Red Wings",
    "Jackets": "Blue Jackets",
    "Knights": "Golden Knights"
    }  
    response = requests.get(url, headers=headers)
    data = response.json()['games']
    count = 0
    games = []
    for i in data:
        home_short = i['home']['name'].split()[-1]
        away_short = i['away']['name'].split()[-1]
        if home_short in name_fix:
            home_short = name_fix[home_short]
        if away_short in name_fix:
            away_short = name_fix[away_short]
        games.append({
            "Record": count,
            "Date": pd.to_datetime(i['scheduled'], utc=True),
            "home_team": i['home']['name'],
            "home_alias": i['home']['alias'],
            "home_id": i['home']['id'],
            "away_team": i['away']['name'],
            "away_alias": i['away']['alias'],
            "away_id": i['away']['id'],
            "season_year": '2025',
            "home_team_short": home_short,
            "away_team_short": away_short
        })
        count += 1
    df = pd.DataFrame(games)
    engine_url = f"postgresql+psycopg2://postgres:{PASSWORD}@localhost:5432/nhl_db"
    engine = create_engine(engine_url)
    df.to_sql(
        name="season_schedule",
        con=engine,
        schema="init",
        if_exists="append",
        index=False
    )

def opponent_lookup_nextgame(team):
    lookup_name = team
    today = datetime.now(timezone.utc)
    query = text("""
        SELECT
            CASE
                WHEN home_team = :team OR home_team_short = :team THEN away_team_short
                ELSE home_team_short
            END AS opponent,
            "Date",
            home_team,
            away_team
        FROM init.season_schedule
        WHERE (home_team = :team OR home_team_short = :team OR away_team_short = :team OR away_team = :team)
        AND "Date" >= :today
        ORDER BY "Date" ASC
        LIMIT 1;
    """)
    engine_url = f"postgresql+psycopg2://postgres:{PASSWORD}@localhost:5432/nhl_db"
    engine = create_engine(engine_url)
    next_game = pd.read_sql(query, engine, params={"team": lookup_name, "today": today})
    if not next_game.empty:
        opponent = next_game.iloc[0]['opponent']
        return opponent
    else:
        return "No upcoming games found."

if __name__ == "__main__":

    # engine_url = f"postgresql+psycopg2://postgres:{PASSWORD}@localhost:5432/nhl_db"
    # df = pull_team_stats('Devils')
    # update_team_stats(df, "init.team_info", engine_url)