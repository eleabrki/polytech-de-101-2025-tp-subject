import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_city_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/communes_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    city_data_df = raw_data_df[[
        "code",
        "nom",
        "population"
    ]]

    city_data_df.rename(columns={
        "code": "id",
        "nom": "name",
        "nb_inhabitants": "population"
    }, inplace=True)
    city_data_df.drop_duplicates(inplace = True)

    city_data_df["created_date"] = date.today()
    print(city_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def consolidate_station_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # === PARIS === 
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df['id'] = raw_data_df['stationcode'] + '_' + raw_data_df['code_insee_commune']
    raw_data_df["address"] = None
    raw_data_df["created_date"] = None

    station_data_df = raw_data_df[[
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]]
    station_data_df.rename(columns={
        "id": "id",
        "stationcode": "code",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status"
    }, inplace=True)
    station_data_df.drop_duplicates(inplace = True)

    station_data_df["created_date"] = date.today()
    print(station_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")

    # === NANTES ===
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["code_insee_commune"] = "44109"
    raw_data_df['id'] = raw_data_df['number'].astype(str) + '_' + raw_data_df['code_insee_commune']
    raw_data_df["address"] = None
    raw_data_df["created_date"] = None
    raw_data_df["status_bool"] = raw_data_df["status"].map({True: "OPEN", False: "CLOSED"})

    station_data_df = raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "code_insee_commune",
        "address",
        "position.lon",
        "position.lat",
        "status_bool",
        "created_date",
        "bike_stands"
    ]]
    station_data_df.rename(columns={
        "id": "id",
        "number": "code",
        "contract_name": "city_name",
        "code_insee_commune": "city_code",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "status_bool": "status",
        "bike_stands": "capacity"
    }, inplace=True)
    station_data_df.drop_duplicates(inplace = True)

    station_data_df["created_date"] = date.today()
    print(station_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")

def consolidate_station_statement_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # === PARIS ===
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df['station_id'] = raw_data_df['stationcode'] + '_' + raw_data_df['code_insee_commune']
    raw_data_df["address"] = None

    station_statement_data_df = raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate"
    ]]
    station_statement_data_df.rename(columns={
        "station_id": "station_id",
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date"
    }, inplace=True)
    station_statement_data_df.drop_duplicates(inplace = True)

    station_statement_data_df["created_date"] = date.today()
    print(station_statement_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;")

    # === NANTES ===
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["code_insee_commune"] = "44109"
    raw_data_df['station_id'] = raw_data_df['number'].astype(str) + '_' + raw_data_df['code_insee_commune']

    station_statement_data_df = raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update"
    ]]
    station_statement_data_df.rename(columns={
        "station_id": "station_id",
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date"
    }, inplace=True)
    station_statement_data_df.drop_duplicates(inplace = True)

    station_statement_data_df["created_date"] = date.today()
    print(station_statement_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;")
