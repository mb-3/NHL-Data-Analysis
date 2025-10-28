import requests, json, sqlite3, psycopg2, os
import pandas as pd
import tkinter as tk
from tkinter import ttk
from pandas import json_normalize
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PW")

print(API_KEY)
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

def update_team_id():
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

# team_dict = update_team_id()

def get_team_info(team_name):
    cursor.execute(f"SELECT team_id from init.team_id WHERE name = '{team_name}';")
    team = cursor.fetchone()
    url = f"https://api.sportradar.com/nhl/trial/v7/en/seasons/2025/REG/teams/{team[0]}/analytics.json"
    response = requests.get(url, headers=headers)
    avg_data = json_normalize(response.json()['own_record']['statistics']['average'])
    tot_data = json_normalize(response.json()['own_record']['statistics']['total'])
    return avg_data, tot_data

# print(get_team_info(team_dict))

if __name__ == "__main__":
    # update_team_id()
    print(get_team_info('Devils'))