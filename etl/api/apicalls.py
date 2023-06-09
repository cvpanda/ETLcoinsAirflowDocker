import json
import redshift_connector
import requests
import time
import os
from pyspark.sql import SparkSession

driver_path = '/home/nagu89/entrega1_coder/working_dir/spark_drivers/postgresql-42.5.2.jar'

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--driver-class-path {driver_path} --jars {driver_path} pyspark-shell'
os.environ['SPARK_CLASSPATH'] = driver_path


class CoinApi:
    def __init__(self):
        self.url = 'https://coinranking1.p.rapidapi.com/coins'
        self.querystring = json.loads(
            '{"referenceCurrencyUuid": "yhjMzLPhuIDl", "timePeriod": "24h","tiers[0]": "1", "orderBy": "marketCap", "orderDirection": "desc", "limit": "50", "offset": "0"}')
        self.header = json.loads(
            '{"X-RapidAPI-Key": "d872672f37msh61163d694fadb48p14d657jsn2fdad2d69b28","X-RapidAPI-Host": "coinranking1.p.rapidapi.com"}')
        self.host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
        self.user = 'nahuelcasagrande_coderhouse'
        self.password = '8aTQc8TE8b'
        self.database = 'data-engineer-database'
        self.schema = 'nahuelcasagrande_coderhouse'
        self.table = 'coinrank'
        self.connection = None

    def transform_data(self, json_data):
        coins = json_data["data"]["coins"]
        rows = [(time.strftime('%Y-%m-%d %H:%M:%S'), coin["uuid"], coin["symbol"], coin["name"], coin["color"],
                 coin["iconUrl"], coin["marketCap"], coin["price"], coin["listedAt"], coin["tier"], coin["change"], coin["rank"]) for coin in coins]

        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("coinRank") \
            .config("spark.jars", driver_path) \
            .config("spark.driver.extraClassPath", driver_path) \
            .getOrCreate()
        # "spark.driver.extraClassPath",  .config("spark.executor.extraClassPath", driver_path) \
        df = spark.createDataFrame(rows, ["timestamp", "uuid", "symbol", "name", "color",
                                   "iconUrl", "marketCap", "price", "listedAt", "tier", "change", "rank"])
        df.printSchema()
        df.dropDuplicates()
        print("delete duplicates from dataframe")
        return df

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

        df = self.transform_data(json_data)

        # backup write con spark
        # try:
        #     df.write \
        #         .format("jdbc") \
        #         .option("url", f"jdbc:redshift://{self.host}:5439/{self.database}") \
        #         .option("dbtable", f"{self.schema}.{self.table}") \
        #         .option("user", self.user) \
        #         .option("password", self.password) \
        #         .mode("append") \
        #         .save()

        insert_query = f"""
            INSERT INTO {self.schema}.{self.table} (timestamp, uuid, symbol, name, color, iconUrl, marketCap, price, listedAt, tier, change, rank)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        try:
            with self.connection.cursor() as cursor:
                for row in df.rdd.toLocalIterator():
                    values = (
                        # Add the current timestamp
                        time.strftime('%Y-%m-%d %H:%M:%S'),
                        row["uuid"],
                        row["symbol"],
                        row["name"],
                        row["color"],
                        row["iconUrl"],
                        row["marketCap"],
                        row["price"],
                        row["listedAt"],
                        row["tier"],
                        row["change"],
                        row["rank"]
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
