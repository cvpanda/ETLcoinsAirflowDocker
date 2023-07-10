import json
import redshift_connector
import requests
import time
import os
from pyspark.sql import SparkSession
from dotenv import dotenv_values
from pyspark.sql.functions import round
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col

# '/home/nagu89/entrega1_coder/working_dir/spark_drivers/postgresql-42.5.2.jar'
driver_path = os.environ['DRIVER_PATH']

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--driver-class-path {driver_path} --jars {driver_path} pyspark-shell'
os.environ['SPARK_CLASSPATH'] = driver_path


class CoinApi:
    def __init__(self):
        self.url = os.environ['API_URL']
        self.querystring = json.loads(os.environ['QUERYSTRING'])
        self.header = json.loads(os.environ['HEADERS'])
        self.host = os.environ['REDSHIFT_HOST']
        self.user = os.environ['REDSHIFT_USER']
        self.password = os.environ['REDSHIFT_PASS']
        self.database = os.environ['REDSHIFT_DB']
        self.schema = os.environ['REDSHIFT_SCHEMA']
        self.table = 'coinrank'
        self.connection = None
        self.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("coinRank") \
            .config("spark.jars", driver_path) \
            .config("spark.driver.extraClassPath", driver_path) \
            .getOrCreate()
        # "spark.driver.extraClassPath",  .config("spark.executor.extraClassPath", driver_path) \

    def transform_data(self, json_data):
        coins = json_data["data"]["coins"]
        rows = [(time.strftime('%Y-%m-%d %H:%M:%S'), coin["uuid"], coin["symbol"], coin["name"], coin["color"],
                 coin["iconUrl"], coin["marketCap"], coin["price"], coin["listedAt"], coin["tier"], coin["change"], coin["rank"]) for coin in coins]

        schema = StructType([
            StructField("timestamp", StringType(), nullable=False),
            StructField("uuid", StringType(), nullable=False),
            StructField("symbol", StringType(), nullable=True),
            StructField("name", StringType(), nullable=False),
            StructField("color", StringType(), nullable=True),
            StructField("iconUrl", StringType(), nullable=True),
            StructField("marketCap", StringType(), nullable=False),
            StructField("price", StringType(), nullable=False),
            StructField("listedAt", StringType(), nullable=False),
            StructField("tier", StringType(), nullable=False),
            StructField("change", StringType(), nullable=False),
            StructField("rank", StringType(), nullable=False),
        ])

        df = self.spark.createDataFrame(rows, schema)
        df.printSchema()

        # df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

        print("delete duplicates from dataframe")
        df = df.dropDuplicates()

        print("adding transaction fee column")
        dftransactionFee = round(round(df["price"], 2) * 0.1, 2)
        df = df.withColumn(
            "transactionFee", dftransactionFee.cast("decimal(10,2)"))
        df.printSchema()
        df.show()

        return df

    def insert_db(self):
        if not self.connection:
            self.connect_db()

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
            INSERT INTO {self.schema}.{self.table} (timestamp, uuid, symbol, name, color, iconUrl, marketCap, price, listedAt, tier, change, rank, transactionFee)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);
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
                        row["rank"],
                        row["transactionFee"]
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


# Create an instance of the CoinApi class

def main():
    api = CoinApi()
    print("Iniciando..")
    try:
        api.insert_db()
        print("Se inserto correctamente")
    except Exception as e:
        print(f"Algo paso: {str(e)}")
    finally:
        api.close_connection()


if __name__ == "__main__":
    main()
