completar .env con variables

API_URL = 'https://coinranking1.p.rapidapi.com/coins'
QUERYSTRING = '{"referenceCurrencyUuid": "yhjMzLPhuIDl", "timePeriod": "24h","tiers[0]": "1", "orderBy": "marketCap", "orderDirection": "desc", "limit": "50", "offset": "0"}'

HEADERS = '{"X-RapidAPI-Key": "d872672f37msh61163d694fadb48p14d657jsn2fdad2d69b28","X-RapidAPI-Host": "coinranking1.p.rapidapi.com"}'      
REDSHIFT_HOST = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
REDSHIFT_USER = 
REDSHIFT_PASS = 
REDSHIFT_DB = 'data-engineer-database'
REDSHIFT_SCHEMA = 
REDSHIFT_DRIVER = 'io.github.spark_redshift_community.spark.redshift'
REDSHIFT_PORT = '5439'
REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASS}"
DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar

docker-compose up --build

crear variables y conexiones en airflow

En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Spark:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`
    * Extra: `{"queue": "default"}`
En la pestaña `Admin -> Variables` importar archivo dentro de carpeta variables_airflow o crear manualmente
    crear una nueva variable con los siguientes datos:
    * Key: `driver_class_path`
    * Value: `/tmp/drivers/postgresql-42.5.2.jar`
En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `spark_script_dir`
    * Value: `/opt/airflow/scripts`



