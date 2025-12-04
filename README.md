# Sujet de travaux pratiques "Introduction à la data ingénierie"

Le but de ce projet est de créer un pipeline ETL d'ingestion, de transformation et de stockage de données pour mettre en pratique les connaissances acquises lors du cours d'introduction à la data ingénierie. Ce sujet présenté propose d'utiliser les données d'utilisation des bornes de vélos open-sources et "temps réel" dans les grandes villes de France.

Le sujet propose une base qui est un pipeline ETL complet qui couvre la récupération, le stockage et la transformation d'une partie des données de la ville de Paris.

Le but du sujet de travaux pratiques est d'ajouter à ce pipeline des données de consolidation, de dimensions et de faits pour la ville de Paris, ainsi que les données provenant d'autres grandes villes de France. Ces données sont disponibles pour les villes de Nantes, de Toulouse ou encore de Strasbourg. Il faudra aussi enrichir ces données avec les données descriptives des villes de France, via une API de l'État français open-source.

## Explication du code existant

Le projet est découpé en 3 parties :

1. Un fichier python pour récupérer et stocker les données dans des fichiers localement

2. Un fichier python pour consolider les données et faire un premier load dans une base de données type data-warehouse

3. Un fichier python pour agréger les données et créer une modélisation de type dimensionnelle

### Ingestion des données

```python
def get_paris_realtime_bicycle_data():
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    response = requests.request("GET", url)
    serialize_data(response.text, "paris_realtime_bicycle_data.json")

def serialize_data(raw_json: str, file_name: str):
    today_date = datetime.now().strftime("%Y-%m-%d")
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)
```

Ces fonctions python sont assez simples. Elles récupèrent les données sur une API open-source, et les stockent dans un fichier json localement. Ces fonctions sont dans le fichier python `data_ingestion.py`.

J'ai ajouté la ville de Nantes :

```python
def get_nantes_realtime_bicycle_data():
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    response = requests.request("GET", url)
    serialize_data(response.text, "nantes_realtime_bicycle_data.json")
```

J'ai aussi remplacé la source de données des tables CONSOLIDATE_CITY et DIM_CITY par les données provenant d'une API :

```python
def get_communes_data():
    url = "https://geo.api.gouv.fr/communes"
    response = requests.request("GET", url)
    serialize_data(response.text, "communes_data.json")
```

### Consolidation des données

**Duckdb** est une base de données de type data-warehouse que l'on peut utiliser localement, car elle charge les données en "in-memory" ou dans un fichier local. C'est l'équivalent de SQLite, mais pour des bases de données de type data-warehouse. Créer une connexion duckdb permet de "créer" une database et d'interagir avec comme avec un vrai data-warehouse. C'est parfait pour des projets de ce type. Plus d'informations sur le site officiel de duckdb : https://duckdb.org/. N'oubliez pas d'installer le CLI DuckDB via cette documentation : https://duckdb.org/install/?platform=macos&environment=cli. Le CLI vous sera utile plus tard.

Dans le fichier `data_consolidation.py` on trouve une fonction qui permet de créer les tables dans une base de données **duckdb**. On utilise le fichier `create_consolidate_tables.sql` pour définir les schémas des tables. Vous ne devriez pas avoir à modifier les schémas des tables, mais vous pouvez le faire si vous voyez une optimisation à faire ou si le schéma est contraignant pour vous pour la réalisation de ce TP.

```python
def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
    for statement in statements.split(";"):
        print(statement)
        con.execute(statement)
```

Une fois les tables créées, on peut lancer les fonctions de consolidations. Un exemple est donné dans le fichier `data_consolidation.py` :

```python
def consolidate_city_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None

    city_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]
    city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)
    city_data_df.drop_duplicates(inplace = True)

    city_data_df["created_date"] = date.today()
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
```

Explication de cette fonction :

- On commence par créer une connexion à la base duckdb (`read_only = False`) car on va insérer les données dans la base.

- On charge les données depuis les fichiers JSON locaux que l'on a ingérés dans l'étape précédente dans un dataframe Pandas.

- On travaille notre dataframe pour :
  - renommer les colonnes
  - supprimer les colonnes inutiles
  - ajouter des colonnes qui sont attendues par la table `CONSOLIDATE_CITY` dans notre base de données (ici `nb_inhabitants`)

- On insère les données dans la base directement depuis le dataframe (fonctionnalité de duckdb, voir la documentation).

**ATTENTION** : Lors de l'insertion de données dans une table duckdb avec une requête SQL `INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;`, il faut s'assurer que :

- votre dataframe contient le même nombre de colonnes que la table dans la base de données
- les colonnes dans votre dataframe et dans la table doivent être dans le même ordre

**ATTENTION 2** : Les données sont historisées dans les tables de consolidation (d'où la présence des colonnes `created_date` et `id` ou `station_id`). C'est uniquement un choix de conception. Vous pouvez changer ce comportement et supprimer / recharger les données à chaque fois.

Les autres fonctions de consolidation pour les autres tables devraient être similaires.

J'ai modifié la fonction consolidate_city_data pour ajouter les données provenant de l'API "communes", pour avoir accès au nombre d'habitants :

```python
def consolidate_city_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/communes_data.json") as fd:
        data = json.load(fd)

    raw_data_communes_df = pd.json_normalize(data)

    city_data_df = raw_data_communes_df[[
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
```

Pour les deux autres foncitons qui gèrent respectivement les tables consolidate_station et consolidate_station_statement, j'ai utilisé séparément les données de Paris et Nantes pour remplir les tables.

```python
def consolidate_station_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data_paris = {}
    data_nantes = {}

    # === PARIS === 
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data_paris = json.load(fd)

    raw_data_paris_df = pd.json_normalize(data_paris)
    raw_data_paris_df['id'] = raw_data_paris_df['stationcode'] + '_' + raw_data_paris_df['code_insee_commune']
    raw_data_paris_df["address"] = None
    raw_data_paris_df["created_date"] = None

    station_data_df = raw_data_paris_df[[
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
        data_nantes = json.load(fd)

    raw_data_nantes_df = pd.json_normalize(data_nantes)
    raw_data_nantes_df["code_insee_commune"] = "44109"
    raw_data_nantes_df['id'] = raw_data_nantes_df['number'].astype(str) + '_' + raw_data_nantes_df['code_insee_commune']
    raw_data_nantes_df["created_date"] = None
    raw_data_nantes_df["status_bool"] = raw_data_nantes_df["status"].map({"OPEN": "OUI", "CLOSED": "NON"})

    station_data_df = raw_data_nantes_df[[
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
    data_paris = {}
    data_nantes = {}

    # === PARIS ===
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data_paris = json.load(fd)

    raw_data_paris_df = pd.json_normalize(data_paris)
    raw_data_paris_df['station_id'] = raw_data_paris_df['stationcode'] + '_' + raw_data_paris_df['code_insee_commune']
    raw_data_paris_df["address"] = None

    station_statement_data_df = raw_data_paris_df[[
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
        data_nantes = json.load(fd)

    raw_data_nantes_df = pd.json_normalize(data_nantes)
    raw_data_nantes_df["code_insee_commune"] = "44109"
    raw_data_nantes_df['station_id'] = raw_data_nantes_df['number'].astype(str) + '_' + raw_data_nantes_df['code_insee_commune']

    station_statement_data_df = raw_data_nantes_df[[
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
```

### Agrégation des données

Dans le fichier `data_agregation.py` on trouve une fonction qui permet de créer les tables dans une base de données **duckdb**. On utilise le fichier `create_agregate_tables.sql` pour définir les schémas des tables. Ces tables représentent une modélisation dimensionnelle simple :

- Deux tables de dimensions : `dim_city` et `dim_station` qui représentent les données descriptives des villes et des stations de vélos en libre-service.

- Une table de faits : `fact_station_statement` qui représente les relevés de disponibilité des vélos dans les stations.

Vous ne devriez pas avoir à modifier les schémas des tables, mais vous pouvez le faire si vous voyez une optimisation ou si le schéma est contraignant pour vous pour la réalisation de ce TP.

```python
def create_agregate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
    for statement in statements.split(";"):
        print(statement)
        con.execute(statement)
```

Une fois les tables créées, on peut lancer les autres fonctions d'agrégation. Pour les tables de dimensions, les fonctions sont assez simples. Comme vous pouvez le voir dans le fichier exemple `data_agregation.py`:

```python
def agregate_dim_city():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)
```

L'autre table de dimension fonctionne de la même manière :

```python
def agregate_dim_station():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT 
        ID,
        CODE,
        NAME,
        ADDRESS, 
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """

    con.execute(sql_statement)
```

La fonction pour la table `fact_station_statement` sera plus complexe: elle devra gérer les jointures avec les autres tables pour que les données soient analysables avec les données descriptives des tables de dimensions.

```python
def agregate_fact_station_statement():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
        INSERT OR REPLACE INTO FACT_STATION_STATEMENT
        SELECT 
            sst.STATION_ID,
            c.ID AS CITY_ID,
            sst.BICYCLE_DOCKS_AVAILABLE,
            sst.BICYCLE_AVAILABLE,
            sst.LAST_STATEMENT_DATE,
            sst.CREATED_DATE
        FROM CONSOLIDATE_STATION_STATEMENT sst
        JOIN CONSOLIDATE_STATION s ON sst.STATION_ID = s.ID
        JOIN CONSOLIDATE_CITY c ON s.CITY_CODE = c.ID
        WHERE sst.CREATED_DATE = (
            SELECT MAX(CREATED_DATE) 
            FROM CONSOLIDATE_STATION_STATEMENT
        );
    """

    con.execute(sql_statement)
```

**EXPLICATION**

### Le fichier main.py

Le fichier `main.py` contient le code principal du processus et exécute séquentiellement les différentes fonctions expliquées plus haut.

### Points d'attention:

Les étapes de ce pipeline seront exécutés séquentiellement (voir le fichier `main.py`) car :

- On ne peut pas faire de l'orchestration facilement dans les environnements locaux de Polytech

- Ce n'est pas possible d'avoir des connexions concurrentes sur un cluster Duckdb en lecture / écriture.

Cependant, ce pipeline ETL permet in fine de réaliser des analyses simples des données des stations de vélo en libre service en région parisienne.

### Comment faire fonctionner ce projet?

Pour faire fonctionner ce sujet, c'est assez simple:

```bash 
git clone https://github.com/eleabrki/polytech-de-101-2025-tp-subject

cd polytech-de-101-2025-tp-subject

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

python src/main.py
```

## Sujet du TP

Le but de ce TP est d'enrichir ce pipeline avec les données provenant de le ville de Paris, mais aussi avec les données d'autre villes. Les sources de données disponibles sont :

- [Open data Nantes](https://data.nantesmetropole.fr/explore/dataset/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/api/)

- [Open data Toulouse](https://data.toulouse-metropole.fr/explore/dataset/api-velo-toulouse-temps-reel/api/)

**L'ajout d'une seule source de données est suffisant.**

Aussi, il faut remplacer la source de données des tables `CONSOLIDATE_CITY` et `DIM_CITY` par les données provenant de l'API suivante :

- [Open data communes](https://geo.api.gouv.fr/communes)

Une fois l'acquisition de ces nouvelles données réalisée, il faut enrichir le pipeline avec les étapes suivantes :

- ajouter les données de la nouvelle ville dans la consolidation des tables `CONSOLIDATE_STATION` et `CONSOLIDATE_STATION_STATEMENT`

- remplacer la consolidation de `CONSOLIDATE_CITY` et l'adapter pour utiliser les données des communes récupérées plus haut

- adapter si besoin les processus d'agrégation des tables `DIM_STATION` et `FACT_STATION_STATEMENT` et `DIM_CITY`

Au final, le pipeline ETL manager devrait ressembler à ce qui suit :

![Process final](images/image.png)

Au final, vous devriez être capable de réaliser les requêtes SQL suivantes sur votre base de données DuckDB :

```sql
-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');

-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```

Vous pouvez utiliser la commande `duckdb data/duckdb/mobility_analysis.duckdb` pour ouvrir l'invite de commande DuckDB. 

Le sujet devra être rendu sous la forme d'un repository GitHub avec les instructions nécéssaire pour faire fonctionner correctement le projet. Le projet peut être fait seul ou en duo.

### Barème utilisé pour la notation finale :

- Les ingestions fonctionnent correctement et produisent des fichiers json localement (5 points)

- La consolidation est correctement réalisé avec les nouvelles ingérées (5 points)

- L'agrégation des données est correctement réalisée et les requêtes SQL ci-dessus fonctionnent (5 points)

- Le projet intègre pas seulement les données de la ville de Paris mais aussi les données d'une autre ville (5 points)

- 1 points bonus pour la clarté générale du code (commentaires, noms de variables, etc.)
