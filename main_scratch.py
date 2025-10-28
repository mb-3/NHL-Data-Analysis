
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

post_shot_info(get_shot_info)





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