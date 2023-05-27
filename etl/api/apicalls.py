import json
import redshift_connector
import requests
import time
from dotenv import dotenv_values


class CoinApi:
    def __init__(self):
        config = dotenv_values('.env')
        self.url = config['API_URL']
        self.querystring = json.loads(config['QUERYSTRING'])
        self.header = json.loads(config['HEADERS'])
        self.host = config['REDSHIFT_HOST']
        self.user = config['REDSHIFT_USER']
        self.password = config['REDSHIFT_PASS']
        self.database = config['REDSHIFT_DB']
        self.schema = config['REDSHIFT_SCHEMA']
        self.table = "coinrank"
        self.connection = None

    def create_table_if_not_exists(self):
        columns = ["uuid", "symbol", "name", "color", "iconUrl",
                   "marketCap", "price", "listedAt", "tier", "change", "rank"]
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                timestamp TIMESTAMP,
                {', '.join([f"{column} VARCHAR" for column in columns])}
            );
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e

    def insert_db(self):
        if not self.connection:
            self.connect_db()

        self.create_table_if_not_exists()

        json_data = self.make_api_call()

        if isinstance(json_data, str):
            json_data = json.loads(json_data)

        coins = json_data["data"]["coins"]

        insert_query = f"""
            INSERT INTO {self.schema}.{self.table} (timestamp, uuid, symbol, name, color, iconUrl, marketCap, price, listedAt, tier, change, rank)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        try:
            with self.connection.cursor() as cursor:
                for coin in coins:
                    values = (
                        # Add the current timestamp
                        time.strftime('%Y-%m-%d %H:%M:%S'),
                        coin["uuid"],
                        coin["symbol"],
                        coin["name"],
                        coin["color"],
                        coin["iconUrl"],
                        coin["marketCap"],
                        coin["price"],
                        coin["listedAt"],
                        coin["tier"],
                        coin["change"],
                        coin["rank"],
                    )
                    cursor.execute(insert_query, values)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            self.close_connection()

    def make_api_call(self):
        response = requests.get(
            self.url, headers=self.header, params=self.querystring
        )
        json_data = response.json()
        return json_data

    def get_column_names(self, json_data=None):
        if not json_data:
            json_data = self.make_api_call()

        if isinstance(json_data, str):
            json_data = json.loads(json_data)

        columns = set()
        if isinstance(json_data, dict):
            columns.update(json_data.keys())
        elif isinstance(json_data, list):
            for data in json_data:
                if isinstance(data, dict):
                    columns.update(data.keys())

        return list(columns)

    def connect_db(self):
        print("Conectando a DB")
        self.connection = redshift_connector.connect(
            host=self.host,
            user=self.user,
            port=5439,
            password=self.password,
            database=self.database
        )

    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            print("Conexión a DB Cerrada")
