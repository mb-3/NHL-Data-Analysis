import requests
import json
import sqlite3
import psycopg2

headers = {
    "accept": "application/json",
    "x-api-key": "rhd8fvqpPnwn3VcVlZ3jbLd2DX438GNjInQt22KP"
}

connection = psycopg2.connect(
    host="localhost",
    database="nhl_db",
    user="postgres",
    password="ginger777",
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
    data = response.json()
    return data

# print(get_team_info(team_dict))

if __name__ == "__main__":
    # update_team_id()
    print(get_team_info('Devils')['own_record']['statistics']['average'])