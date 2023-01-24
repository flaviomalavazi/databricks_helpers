# Databricks notebook source
# MAGIC %md
# MAGIC # Como fazer requests utilizando o pyspark da maneira correta?
# MAGIC Neste exemplo, vamos seguir o passo a passo [deste artigo do medium](https://medium.com/geekculture/how-to-execute-a-rest-api-call-on-apache-spark-the-right-way-in-python-4367f2740e78), do James S Hocking, para implementar uma função que executa chamadas REST em uma API e trás os resultados para um dataframe PySpark. [](https://github.com/jamesshocking/Spark-REST-API-UDF).
# MAGIC 
# MAGIC Para evitar que estouremos a taxa máxima de requisições de nossa API (rate limit), vamos utilizar a [biblioteca backoff](https://pypi.org/project/backoff/) do Python, para controlar o ritmo de requisições que faremos em nossa UDF.

# COMMAND ----------

# DBTITLE 1,Instalando a biblioteca
# MAGIC %sh
# MAGIC pip install backoff

# COMMAND ----------

# DBTITLE 1,Importando as bibliotecas e tipos necessários
import requests
import backoff
import json
from pyspark.sql.functions import udf, col, explode, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType, LongType
from pyspark.sql import Row

# COMMAND ----------

# DBTITLE 1,Verificando se nosso escopo de segredos está populado
dbutils.secrets.list("databricks_latam_demos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requisição de exemplo para entendermos a estrutura da resposta da API de clima que utilizaremos
# MAGIC A API que vamos usar neste exemplo é a [Weather API.com](https://rapidapi.com/weatherapi/api/weatherapi-com), que é uma API Freemium e que aceita requisições REST de `get` na estrutura abaixo

# COMMAND ----------

url = "https://weatherapi-com.p.rapidapi.com/history.json"

querystring = {"q":"Sao_Paulo","dt":"2023-01-22","lang":"en"}

headers = {
	"X-RapidAPI-Key": dbutils.secrets.get("databricks_latam_demos", "weather_api_key"),
	"X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

response_dict = json.loads(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### A estrutura da resposta da API que escolhemos para o exemplo é a seguinte:
# MAGIC ```
# MAGIC {'location': {'name': 'Sao Paulo',
# MAGIC   'region': 'Sao Paulo',
# MAGIC   'country': 'Brazil',
# MAGIC   'lat': -23.53,
# MAGIC   'lon': -46.62,
# MAGIC   'tz_id': 'America/Sao_Paulo',
# MAGIC   'localtime_epoch': 1674515581,
# MAGIC   'localtime': '2023-01-23 20:13'},
# MAGIC  'forecast': {'forecastday': [{'date': '2023-01-22',
# MAGIC     'date_epoch': 1674345600,
# MAGIC     'day': {'maxtemp_c': 31.2,
# MAGIC      'maxtemp_f': 88.2,
# MAGIC      'mintemp_c': 19.4,
# MAGIC      'mintemp_f': 66.9,
# MAGIC      'avgtemp_c': 26.6,
# MAGIC      'avgtemp_f': 80.0,
# MAGIC      'maxwind_mph': 8.7,
# MAGIC      'maxwind_kph': 14.0,
# MAGIC      'totalprecip_mm': 1.2,
# MAGIC      'totalprecip_in': 0.05,
# MAGIC      'avgvis_km': 10.0,
# MAGIC      'avgvis_miles': 6.0,
# MAGIC      'avghumidity': 72.0,
# MAGIC      'condition': {'text': 'Partly cloudy',
# MAGIC       'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',
# MAGIC       'code': 1003},
# MAGIC      'uv': 0.0},
# MAGIC     'astro': {'sunrise': '05:38 AM',
# MAGIC      'sunset': '06:58 PM',
# MAGIC      'moonrise': '06:12 AM',
# MAGIC      'moonset': '08:03 PM',
# MAGIC      'moon_phase': 'Waxing Crescent',
# MAGIC      'moon_illumination': '0'},
# MAGIC     'hour': [{'time_epoch': 1674356400,
# MAGIC       'time': '2023-01-22 00:00',
# MAGIC       'temp_c': 20.9,
# MAGIC       'temp_f': 69.6,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Clear',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 3.8,
# MAGIC       'wind_kph': 6.1,
# MAGIC       'wind_degree': 347,
# MAGIC       'wind_dir': 'NNW',
# MAGIC       'pressure_mb': 1009.0,
# MAGIC       'pressure_in': 29.79,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 87,
# MAGIC       'cloud': 6,
# MAGIC       'feelslike_c': 20.9,
# MAGIC       'feelslike_f': 69.6,
# MAGIC       'windchill_c': 20.9,
# MAGIC       'windchill_f': 69.6,
# MAGIC       'heatindex_c': 20.9,
# MAGIC       'heatindex_f': 69.6,
# MAGIC       'dewpoint_c': 18.6,
# MAGIC       'dewpoint_f': 65.5,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 6.3,
# MAGIC       'gust_kph': 10.1},
# MAGIC      {'time_epoch': 1674360000,
# MAGIC       'time': '2023-01-22 01:00',
# MAGIC       'temp_c': 20.6,
# MAGIC       'temp_f': 69.0,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Clear',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 3.8,
# MAGIC       'wind_kph': 6.1,
# MAGIC       'wind_degree': 345,
# MAGIC       'wind_dir': 'NNW',
# MAGIC       'pressure_mb': 1008.0,
# MAGIC       'pressure_in': 29.78,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 88,
# MAGIC       'cloud': 11,
# MAGIC       'feelslike_c': 20.6,
# MAGIC       'feelslike_f': 69.0,
# MAGIC       'windchill_c': 20.6,
# MAGIC       'windchill_f': 69.0,
# MAGIC       'heatindex_c': 20.6,
# MAGIC       'heatindex_f': 69.0,
# MAGIC       'dewpoint_c': 18.5,
# MAGIC       'dewpoint_f': 65.3,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 6.3,
# MAGIC       'gust_kph': 10.1},
# MAGIC      {'time_epoch': 1674363600,
# MAGIC       'time': '2023-01-22 02:00',
# MAGIC       'temp_c': 20.2,
# MAGIC       'temp_f': 68.4,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Clear',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 3.8,
# MAGIC       'wind_kph': 6.1,
# MAGIC       'wind_degree': 343,
# MAGIC       'wind_dir': 'NNW',
# MAGIC       'pressure_mb': 1008.0,
# MAGIC       'pressure_in': 29.77,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 89,
# MAGIC       'cloud': 16,
# MAGIC       'feelslike_c': 20.2,
# MAGIC       'feelslike_f': 68.4,
# MAGIC       'windchill_c': 20.2,
# MAGIC       'windchill_f': 68.4,
# MAGIC       'heatindex_c': 20.2,
# MAGIC       'heatindex_f': 68.4,
# MAGIC       'dewpoint_c': 18.4,
# MAGIC       'dewpoint_f': 65.1,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 6.3,
# MAGIC       'gust_kph': 10.1},
# MAGIC      {'time_epoch': 1674367200,
# MAGIC       'time': '2023-01-22 03:00',
# MAGIC       'temp_c': 19.9,
# MAGIC       'temp_f': 67.8,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Clear',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 3.8,
# MAGIC       'wind_kph': 6.1,
# MAGIC       'wind_degree': 341,
# MAGIC       'wind_dir': 'NNW',
# MAGIC       'pressure_mb': 1008.0,
# MAGIC       'pressure_in': 29.76,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 91,
# MAGIC       'cloud': 21,
# MAGIC       'feelslike_c': 19.9,
# MAGIC       'feelslike_f': 67.8,
# MAGIC       'windchill_c': 19.9,
# MAGIC       'windchill_f': 67.8,
# MAGIC       'heatindex_c': 19.9,
# MAGIC       'heatindex_f': 67.8,
# MAGIC       'dewpoint_c': 18.3,
# MAGIC       'dewpoint_f': 64.9,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 6.3,
# MAGIC       'gust_kph': 10.1},
# MAGIC      {'time_epoch': 1674370800,
# MAGIC       'time': '2023-01-22 04:00',
# MAGIC       'temp_c': 19.7,
# MAGIC       'temp_f': 67.5,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Clear',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 3.7,
# MAGIC       'wind_kph': 6.0,
# MAGIC       'wind_degree': 344,
# MAGIC       'wind_dir': 'NNW',
# MAGIC       'pressure_mb': 1008.0,
# MAGIC       'pressure_in': 29.78,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 91,
# MAGIC       'cloud': 26,
# MAGIC       'feelslike_c': 19.7,
# MAGIC       'feelslike_f': 67.5,
# MAGIC       'windchill_c': 19.7,
# MAGIC       'windchill_f': 67.5,
# MAGIC       'heatindex_c': 19.7,
# MAGIC       'heatindex_f': 67.5,
# MAGIC       'dewpoint_c': 18.2,
# MAGIC       'dewpoint_f': 64.8,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 6.0,
# MAGIC       'gust_kph': 9.7},
# MAGIC      {'time_epoch': 1674374400,
# MAGIC       'time': '2023-01-22 05:00',
# MAGIC       'temp_c': 19.6,
# MAGIC       'temp_f': 67.2,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Partly cloudy',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/116.png',
# MAGIC        'code': 1003},
# MAGIC       'wind_mph': 3.7,
# MAGIC       'wind_kph': 5.9,
# MAGIC       'wind_degree': 347,
# MAGIC       'wind_dir': 'NNW',
# MAGIC       'pressure_mb': 1009.0,
# MAGIC       'pressure_in': 29.79,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 92,
# MAGIC       'cloud': 31,
# MAGIC       'feelslike_c': 19.6,
# MAGIC       'feelslike_f': 67.2,
# MAGIC       'windchill_c': 19.6,
# MAGIC       'windchill_f': 67.2,
# MAGIC       'heatindex_c': 19.6,
# MAGIC       'heatindex_f': 67.2,
# MAGIC       'dewpoint_c': 18.2,
# MAGIC       'dewpoint_f': 64.7,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 1,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 5.8,
# MAGIC       'gust_kph': 9.4},
# MAGIC      {'time_epoch': 1674378000,
# MAGIC       'time': '2023-01-22 06:00',
# MAGIC       'temp_c': 19.4,
# MAGIC       'temp_f': 66.9,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Partly cloudy',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',
# MAGIC        'code': 1003},
# MAGIC       'wind_mph': 3.6,
# MAGIC       'wind_kph': 5.8,
# MAGIC       'wind_degree': 350,
# MAGIC       'wind_dir': 'N',
# MAGIC       'pressure_mb': 1009.0,
# MAGIC       'pressure_in': 29.8,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 92,
# MAGIC       'cloud': 36,
# MAGIC       'feelslike_c': 19.4,
# MAGIC       'feelslike_f': 66.9,
# MAGIC       'windchill_c': 19.4,
# MAGIC       'windchill_f': 66.9,
# MAGIC       'heatindex_c': 19.4,
# MAGIC       'heatindex_f': 66.9,
# MAGIC       'dewpoint_c': 18.1,
# MAGIC       'dewpoint_f': 64.6,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 5.6,
# MAGIC       'gust_kph': 9.0},
# MAGIC      {'time_epoch': 1674381600,
# MAGIC       'time': '2023-01-22 07:00',
# MAGIC       'temp_c': 21.0,
# MAGIC       'temp_f': 69.7,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Partly cloudy',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',
# MAGIC        'code': 1003},
# MAGIC       'wind_mph': 5.1,
# MAGIC       'wind_kph': 8.2,
# MAGIC       'wind_degree': 343,
# MAGIC       'wind_dir': 'NNW',
# MAGIC       'pressure_mb': 1009.0,
# MAGIC       'pressure_in': 29.8,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 86,
# MAGIC       'cloud': 28,
# MAGIC       'feelslike_c': 21.0,
# MAGIC       'feelslike_f': 69.7,
# MAGIC       'windchill_c': 21.0,
# MAGIC       'windchill_f': 69.7,
# MAGIC       'heatindex_c': 21.6,
# MAGIC       'heatindex_f': 70.8,
# MAGIC       'dewpoint_c': 18.5,
# MAGIC       'dewpoint_f': 65.3,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 1,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 6.8,
# MAGIC       'gust_kph': 10.9},
# MAGIC      {'time_epoch': 1674385200,
# MAGIC       'time': '2023-01-22 08:00',
# MAGIC       'temp_c': 22.5,
# MAGIC       'temp_f': 72.6,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Sunny',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 6.6,
# MAGIC       'wind_kph': 10.6,
# MAGIC       'wind_degree': 335,
# MAGIC       'wind_dir': 'NNW',
# MAGIC       'pressure_mb': 1009.0,
# MAGIC       'pressure_in': 29.81,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 80,
# MAGIC       'cloud': 20,
# MAGIC       'feelslike_c': 23.7,
# MAGIC       'feelslike_f': 74.7,
# MAGIC       'windchill_c': 22.5,
# MAGIC       'windchill_f': 72.6,
# MAGIC       'heatindex_c': 23.7,
# MAGIC       'heatindex_f': 74.7,
# MAGIC       'dewpoint_c': 18.9,
# MAGIC       'dewpoint_f': 66.0,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 8.0,
# MAGIC       'gust_kph': 12.8},
# MAGIC      {'time_epoch': 1674388800,
# MAGIC       'time': '2023-01-22 09:00',
# MAGIC       'temp_c': 24.1,
# MAGIC       'temp_f': 75.4,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Sunny',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 8.1,
# MAGIC       'wind_kph': 13.0,
# MAGIC       'wind_degree': 328,
# MAGIC       'wind_dir': 'NNW',
# MAGIC       'pressure_mb': 1010.0,
# MAGIC       'pressure_in': 29.81,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 74,
# MAGIC       'cloud': 12,
# MAGIC       'feelslike_c': 25.9,
# MAGIC       'feelslike_f': 78.6,
# MAGIC       'windchill_c': 24.1,
# MAGIC       'windchill_f': 75.4,
# MAGIC       'heatindex_c': 25.9,
# MAGIC       'heatindex_f': 78.6,
# MAGIC       'dewpoint_c': 19.3,
# MAGIC       'dewpoint_f': 66.7,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 9.2,
# MAGIC       'gust_kph': 14.8},
# MAGIC      {'time_epoch': 1674392400,
# MAGIC       'time': '2023-01-22 10:00',
# MAGIC       'temp_c': 26.2,
# MAGIC       'temp_f': 79.1,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Sunny',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 8.3,
# MAGIC       'wind_kph': 13.3,
# MAGIC       'wind_degree': 322,
# MAGIC       'wind_dir': 'NW',
# MAGIC       'pressure_mb': 1009.0,
# MAGIC       'pressure_in': 29.79,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 67,
# MAGIC       'cloud': 17,
# MAGIC       'feelslike_c': 28.0,
# MAGIC       'feelslike_f': 82.3,
# MAGIC       'windchill_c': 26.2,
# MAGIC       'windchill_f': 79.1,
# MAGIC       'heatindex_c': 28.0,
# MAGIC       'heatindex_f': 82.3,
# MAGIC       'dewpoint_c': 19.2,
# MAGIC       'dewpoint_f': 66.6,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 9.5,
# MAGIC       'gust_kph': 15.2},
# MAGIC      {'time_epoch': 1674396000,
# MAGIC       'time': '2023-01-22 11:00',
# MAGIC       'temp_c': 28.2,
# MAGIC       'temp_f': 82.8,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Partly cloudy',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',
# MAGIC        'code': 1003},
# MAGIC       'wind_mph': 8.5,
# MAGIC       'wind_kph': 13.7,
# MAGIC       'wind_degree': 317,
# MAGIC       'wind_dir': 'NW',
# MAGIC       'pressure_mb': 1008.0,
# MAGIC       'pressure_in': 29.78,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 59,
# MAGIC       'cloud': 23,
# MAGIC       'feelslike_c': 30.0,
# MAGIC       'feelslike_f': 86.1,
# MAGIC       'windchill_c': 28.2,
# MAGIC       'windchill_f': 82.8,
# MAGIC       'heatindex_c': 30.0,
# MAGIC       'heatindex_f': 86.1,
# MAGIC       'dewpoint_c': 19.2,
# MAGIC       'dewpoint_f': 66.5,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 1,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 9.8,
# MAGIC       'gust_kph': 15.7},
# MAGIC      {'time_epoch': 1674399600,
# MAGIC       'time': '2023-01-22 12:00',
# MAGIC       'temp_c': 30.3,
# MAGIC       'temp_f': 86.5,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Partly cloudy',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',
# MAGIC        'code': 1003},
# MAGIC       'wind_mph': 8.7,
# MAGIC       'wind_kph': 14.0,
# MAGIC       'wind_degree': 311,
# MAGIC       'wind_dir': 'NW',
# MAGIC       'pressure_mb': 1008.0,
# MAGIC       'pressure_in': 29.76,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 51,
# MAGIC       'cloud': 28,
# MAGIC       'feelslike_c': 32.1,
# MAGIC       'feelslike_f': 89.8,
# MAGIC       'windchill_c': 30.3,
# MAGIC       'windchill_f': 86.5,
# MAGIC       'heatindex_c': 32.1,
# MAGIC       'heatindex_f': 89.8,
# MAGIC       'dewpoint_c': 19.1,
# MAGIC       'dewpoint_f': 66.4,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 10.1,
# MAGIC       'gust_kph': 16.2},
# MAGIC      {'time_epoch': 1674403200,
# MAGIC       'time': '2023-01-22 13:00',
# MAGIC       'temp_c': 30.6,
# MAGIC       'temp_f': 87.1,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Partly cloudy',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',
# MAGIC        'code': 1003},
# MAGIC       'wind_mph': 8.5,
# MAGIC       'wind_kph': 13.7,
# MAGIC       'wind_degree': 304,
# MAGIC       'wind_dir': 'NW',
# MAGIC       'pressure_mb': 1007.0,
# MAGIC       'pressure_in': 29.74,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 49,
# MAGIC       'cloud': 34,
# MAGIC       'feelslike_c': 32.2,
# MAGIC       'feelslike_f': 89.9,
# MAGIC       'windchill_c': 30.6,
# MAGIC       'windchill_f': 87.1,
# MAGIC       'heatindex_c': 32.2,
# MAGIC       'heatindex_f': 89.9,
# MAGIC       'dewpoint_c': 18.5,
# MAGIC       'dewpoint_f': 65.3,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 1,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 9.8,
# MAGIC       'gust_kph': 15.7},
# MAGIC      {'time_epoch': 1674406800,
# MAGIC       'time': '2023-01-22 14:00',
# MAGIC       'temp_c': 30.9,
# MAGIC       'temp_f': 87.6,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Partly cloudy',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',
# MAGIC        'code': 1003},
# MAGIC       'wind_mph': 8.3,
# MAGIC       'wind_kph': 13.3,
# MAGIC       'wind_degree': 298,
# MAGIC       'wind_dir': 'WNW',
# MAGIC       'pressure_mb': 1006.0,
# MAGIC       'pressure_in': 29.72,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 46,
# MAGIC       'cloud': 39,
# MAGIC       'feelslike_c': 32.2,
# MAGIC       'feelslike_f': 90.0,
# MAGIC       'windchill_c': 30.9,
# MAGIC       'windchill_f': 87.6,
# MAGIC       'heatindex_c': 32.2,
# MAGIC       'heatindex_f': 90.0,
# MAGIC       'dewpoint_c': 17.9,
# MAGIC       'dewpoint_f': 64.2,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 1,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 9.5,
# MAGIC       'gust_kph': 15.2},
# MAGIC      {'time_epoch': 1674410400,
# MAGIC       'time': '2023-01-22 15:00',
# MAGIC       'temp_c': 31.2,
# MAGIC       'temp_f': 88.2,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Partly cloudy',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',
# MAGIC        'code': 1003},
# MAGIC       'wind_mph': 8.1,
# MAGIC       'wind_kph': 13.0,
# MAGIC       'wind_degree': 291,
# MAGIC       'wind_dir': 'WNW',
# MAGIC       'pressure_mb': 1006.0,
# MAGIC       'pressure_in': 29.69,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 43,
# MAGIC       'cloud': 45,
# MAGIC       'feelslike_c': 32.3,
# MAGIC       'feelslike_f': 90.1,
# MAGIC       'windchill_c': 31.2,
# MAGIC       'windchill_f': 88.2,
# MAGIC       'heatindex_c': 32.3,
# MAGIC       'heatindex_f': 90.1,
# MAGIC       'dewpoint_c': 17.3,
# MAGIC       'dewpoint_f': 63.1,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 9.2,
# MAGIC       'gust_kph': 14.8},
# MAGIC      {'time_epoch': 1674414000,
# MAGIC       'time': '2023-01-22 16:00',
# MAGIC       'temp_c': 30.2,
# MAGIC       'temp_f': 86.4,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Partly cloudy',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',
# MAGIC        'code': 1003},
# MAGIC       'wind_mph': 7.3,
# MAGIC       'wind_kph': 11.8,
# MAGIC       'wind_degree': 302,
# MAGIC       'wind_dir': 'WNW',
# MAGIC       'pressure_mb': 1005.0,
# MAGIC       'pressure_in': 29.69,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 48,
# MAGIC       'cloud': 36,
# MAGIC       'feelslike_c': 31.5,
# MAGIC       'feelslike_f': 88.6,
# MAGIC       'windchill_c': 30.2,
# MAGIC       'windchill_f': 86.4,
# MAGIC       'heatindex_c': 31.5,
# MAGIC       'heatindex_f': 88.6,
# MAGIC       'dewpoint_c': 17.9,
# MAGIC       'dewpoint_f': 64.2,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 1,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 8.4,
# MAGIC       'gust_kph': 13.6},
# MAGIC      {'time_epoch': 1674417600,
# MAGIC       'time': '2023-01-22 17:00',
# MAGIC       'temp_c': 29.2,
# MAGIC       'temp_f': 84.6,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Sunny',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 6.6,
# MAGIC       'wind_kph': 10.6,
# MAGIC       'wind_degree': 312,
# MAGIC       'wind_dir': 'NW',
# MAGIC       'pressure_mb': 1005.0,
# MAGIC       'pressure_in': 29.68,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 53,
# MAGIC       'cloud': 26,
# MAGIC       'feelslike_c': 30.6,
# MAGIC       'feelslike_f': 87.1,
# MAGIC       'windchill_c': 29.2,
# MAGIC       'windchill_f': 84.6,
# MAGIC       'heatindex_c': 30.6,
# MAGIC       'heatindex_f': 87.1,
# MAGIC       'dewpoint_c': 18.5,
# MAGIC       'dewpoint_f': 65.3,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 7.7,
# MAGIC       'gust_kph': 12.4},
# MAGIC      {'time_epoch': 1674421200,
# MAGIC       'time': '2023-01-22 18:00',
# MAGIC       'temp_c': 28.2,
# MAGIC       'temp_f': 82.8,
# MAGIC       'is_day': 1,
# MAGIC       'condition': {'text': 'Sunny',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/day/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 5.8,
# MAGIC       'wind_kph': 9.4,
# MAGIC       'wind_degree': 322,
# MAGIC       'wind_dir': 'NW',
# MAGIC       'pressure_mb': 1005.0,
# MAGIC       'pressure_in': 29.68,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 58,
# MAGIC       'cloud': 16,
# MAGIC       'feelslike_c': 29.8,
# MAGIC       'feelslike_f': 85.6,
# MAGIC       'windchill_c': 28.2,
# MAGIC       'windchill_f': 82.8,
# MAGIC       'heatindex_c': 29.8,
# MAGIC       'heatindex_f': 85.6,
# MAGIC       'dewpoint_c': 19.1,
# MAGIC       'dewpoint_f': 66.4,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 6.9,
# MAGIC       'gust_kph': 11.2},
# MAGIC      {'time_epoch': 1674424800,
# MAGIC       'time': '2023-01-22 19:00',
# MAGIC       'temp_c': 26.8,
# MAGIC       'temp_f': 80.2,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Clear',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/113.png',
# MAGIC        'code': 1000},
# MAGIC       'wind_mph': 5.0,
# MAGIC       'wind_kph': 8.0,
# MAGIC       'wind_degree': 308,
# MAGIC       'wind_dir': 'NW',
# MAGIC       'pressure_mb': 1006.0,
# MAGIC       'pressure_in': 29.71,
# MAGIC       'precip_mm': 0.0,
# MAGIC       'precip_in': 0.0,
# MAGIC       'humidity': 65,
# MAGIC       'cloud': 40,
# MAGIC       'feelslike_c': 28.5,
# MAGIC       'feelslike_f': 83.3,
# MAGIC       'windchill_c': 26.8,
# MAGIC       'windchill_f': 80.2,
# MAGIC       'heatindex_c': 28.5,
# MAGIC       'heatindex_f': 83.3,
# MAGIC       'dewpoint_c': 19.5,
# MAGIC       'dewpoint_f': 67.2,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 6.4,
# MAGIC       'gust_kph': 10.3},
# MAGIC      {'time_epoch': 1674428400,
# MAGIC       'time': '2023-01-22 20:00',
# MAGIC       'temp_c': 25.3,
# MAGIC       'temp_f': 77.6,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Light rain shower',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/353.png',
# MAGIC        'code': 1240},
# MAGIC       'wind_mph': 4.2,
# MAGIC       'wind_kph': 6.7,
# MAGIC       'wind_degree': 293,
# MAGIC       'wind_dir': 'WNW',
# MAGIC       'pressure_mb': 1007.0,
# MAGIC       'pressure_in': 29.74,
# MAGIC       'precip_mm': 0.4,
# MAGIC       'precip_in': 0.02,
# MAGIC       'humidity': 73,
# MAGIC       'cloud': 63,
# MAGIC       'feelslike_c': 27.2,
# MAGIC       'feelslike_f': 81.0,
# MAGIC       'windchill_c': 25.3,
# MAGIC       'windchill_f': 77.6,
# MAGIC       'heatindex_c': 27.2,
# MAGIC       'heatindex_f': 81.0,
# MAGIC       'dewpoint_c': 20.0,
# MAGIC       'dewpoint_f': 67.9,
# MAGIC       'will_it_rain': 1,
# MAGIC       'chance_of_rain': 80,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 5.9,
# MAGIC       'gust_kph': 9.5},
# MAGIC      {'time_epoch': 1674432000,
# MAGIC       'time': '2023-01-22 21:00',
# MAGIC       'temp_c': 23.9,
# MAGIC       'temp_f': 75.0,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Light rain shower',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/353.png',
# MAGIC        'code': 1240},
# MAGIC       'wind_mph': 3.4,
# MAGIC       'wind_kph': 5.4,
# MAGIC       'wind_degree': 279,
# MAGIC       'wind_dir': 'W',
# MAGIC       'pressure_mb': 1008.0,
# MAGIC       'pressure_in': 29.77,
# MAGIC       'precip_mm': 0.53,
# MAGIC       'precip_in': 0.02,
# MAGIC       'humidity': 81,
# MAGIC       'cloud': 87,
# MAGIC       'feelslike_c': 25.9,
# MAGIC       'feelslike_f': 78.6,
# MAGIC       'windchill_c': 23.9,
# MAGIC       'windchill_f': 75.0,
# MAGIC       'heatindex_c': 25.9,
# MAGIC       'heatindex_f': 78.6,
# MAGIC       'dewpoint_c': 20.4,
# MAGIC       'dewpoint_f': 68.7,
# MAGIC       'will_it_rain': 0,
# MAGIC       'chance_of_rain': 0,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 10.0,
# MAGIC       'vis_miles': 6.0,
# MAGIC       'gust_mph': 5.4,
# MAGIC       'gust_kph': 8.6},
# MAGIC      {'time_epoch': 1674435600,
# MAGIC       'time': '2023-01-22 22:00',
# MAGIC       'temp_c': 23.3,
# MAGIC       'temp_f': 74.0,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Light rain shower',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/353.png',
# MAGIC        'code': 1240},
# MAGIC       'wind_mph': 3.4,
# MAGIC       'wind_kph': 5.4,
# MAGIC       'wind_degree': 260,
# MAGIC       'wind_dir': 'W',
# MAGIC       'pressure_mb': 1009.0,
# MAGIC       'pressure_in': 29.79,
# MAGIC       'precip_mm': 0.27,
# MAGIC       'precip_in': 0.01,
# MAGIC       'humidity': 82,
# MAGIC       'cloud': 87,
# MAGIC       'feelslike_c': 25.5,
# MAGIC       'feelslike_f': 77.8,
# MAGIC       'windchill_c': 23.3,
# MAGIC       'windchill_f': 74.0,
# MAGIC       'heatindex_c': 25.5,
# MAGIC       'heatindex_f': 77.8,
# MAGIC       'dewpoint_c': 20.2,
# MAGIC       'dewpoint_f': 68.3,
# MAGIC       'will_it_rain': 1,
# MAGIC       'chance_of_rain': 80,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 9.0,
# MAGIC       'vis_miles': 5.0,
# MAGIC       'gust_mph': 5.5,
# MAGIC       'gust_kph': 8.9},
# MAGIC      {'time_epoch': 1674439200,
# MAGIC       'time': '2023-01-22 23:00',
# MAGIC       'temp_c': 22.8,
# MAGIC       'temp_f': 73.0,
# MAGIC       'is_day': 0,
# MAGIC       'condition': {'text': 'Moderate or heavy rain shower',
# MAGIC        'icon': '//cdn.weatherapi.com/weather/64x64/night/356.png',
# MAGIC        'code': 1243},
# MAGIC       'wind_mph': 3.4,
# MAGIC       'wind_kph': 5.4,
# MAGIC       'wind_degree': 241,
# MAGIC       'wind_dir': 'WSW',
# MAGIC       'pressure_mb': 1009.0,
# MAGIC       'pressure_in': 29.8,
# MAGIC       'precip_mm': 1.07,
# MAGIC       'precip_in': 0.04,
# MAGIC       'humidity': 84,
# MAGIC       'cloud': 88,
# MAGIC       'feelslike_c': 25.0,
# MAGIC       'feelslike_f': 77.1,
# MAGIC       'windchill_c': 22.8,
# MAGIC       'windchill_f': 73.0,
# MAGIC       'heatindex_c': 25.0,
# MAGIC       'heatindex_f': 77.1,
# MAGIC       'dewpoint_c': 19.9,
# MAGIC       'dewpoint_f': 67.9,
# MAGIC       'will_it_rain': 1,
# MAGIC       'chance_of_rain': 81,
# MAGIC       'will_it_snow': 0,
# MAGIC       'chance_of_snow': 0,
# MAGIC       'vis_km': 8.0,
# MAGIC       'vis_miles': 4.0,
# MAGIC       'gust_mph': 5.7,
# MAGIC       'gust_kph': 9.1}]}]}}
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sabendo a estrutura da resposta da API, definimos como será o schema que nossa UDF deve retornar
# MAGIC A definição de schema é fundamental para as UDFs pois este tipo de função exige uma estrutura pré definida para retornar resultados corretamente

# COMMAND ----------

schema = StructType(
    [
        StructField(
            "location",
            StructType(
                [
                    StructField("name", StringType()),
                    StructField("region", StringType()),
                    StructField("country", StringType()),
                    StructField("lat", FloatType()),
                    StructField("lon", FloatType()),
                    StructField("tz_id", StringType()),
                    StructField("localtime_epoch", LongType()),
                    StructField("localtime", StringType()),
                ]
            ),
        ),
        StructField(
            "forecast",
            StructType(
                [
                    StructField(
                        "forecastday",
                        ArrayType(
                            StructType(
                                [
                                    StructField("date", StringType()),
                                    StructField("date_epoch", LongType()),
                                    StructField(
                                        "day",
                                        StructType(
                                            [
                                                StructField("maxtemp_c", FloatType()),
                                                StructField("maxtemp_f", FloatType()),
                                                StructField("mintemp_c", FloatType()),
                                                StructField("mintemp_f", FloatType()),
                                                StructField("avgtemp_c", FloatType()),
                                                StructField("avgtemp_f", FloatType()),
                                                StructField("maxwind_mph", FloatType()),
                                                StructField("maxwind_kph", FloatType()),
                                                StructField(
                                                    "totalprecip_mm", FloatType()
                                                ),
                                                StructField(
                                                    "totalprecip_in", FloatType()
                                                ),
                                                StructField("avgvis_km", FloatType()),
                                                StructField(
                                                    "avgvis_miles", FloatType()
                                                ),
                                                StructField("avghumidity", FloatType()),
                                                StructField(
                                                    "condition",
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "text", StringType()
                                                            ),
                                                            StructField(
                                                                "icon", StringType()
                                                            ),
                                                            StructField(
                                                                "code", IntegerType()
                                                            ),
                                                        ]
                                                    ),
                                                ),
                                                StructField("uv", FloatType()),
                                            ]
                                        ),
                                    ),
                                    StructField(
                                        "astro",
                                        StructType(
                                            [
                                                StructField("sunrise", StringType()),
                                                StructField("sunset", StringType()),
                                                StructField("moonrise", StringType()),
                                                StructField("moonset", StringType()),
                                                StructField("moon_phase", StringType()),
                                                StructField(
                                                    "moon_illumination", StringType()
                                                ),
                                            ]
                                        ),
                                    ),
                                    StructField(
                                        "hour",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        "time_epoch", LongType()
                                                    ),
                                                    StructField("time", StringType()),
                                                    StructField("temp_c", FloatType()),
                                                    StructField("temp_f", FloatType()),
                                                    StructField("is_day", FloatType()),
                                                    StructField(
                                                        "wind_mph", FloatType()
                                                    ),
                                                    StructField(
                                                        "wind_kph", FloatType()
                                                    ),
                                                    StructField(
                                                        "wind_degree", FloatType()
                                                    ),
                                                    StructField(
                                                        "wind_dir", StringType()
                                                    ),
                                                    StructField(
                                                        "pressure_mb", FloatType()
                                                    ),
                                                    StructField(
                                                        "pressure_in", FloatType()
                                                    ),
                                                    StructField(
                                                        "precip_mm", FloatType()
                                                    ),
                                                    StructField(
                                                        "precip_in", FloatType()
                                                    ),
                                                    StructField(
                                                        "humidity", FloatType()
                                                    ),
                                                    StructField("cloud", FloatType()),
                                                    StructField(
                                                        "feelslike_c", FloatType()
                                                    ),
                                                    StructField(
                                                        "feelslike_f", FloatType()
                                                    ),
                                                    StructField(
                                                        "windchill_c", FloatType()
                                                    ),
                                                    StructField(
                                                        "windchill_f", FloatType()
                                                    ),
                                                    StructField(
                                                        "heatindex_c", FloatType()
                                                    ),
                                                    StructField(
                                                        "heatindex_f", FloatType()
                                                    ),
                                                    StructField(
                                                        "dewpoint_c", FloatType()
                                                    ),
                                                    StructField(
                                                        "dewpoint_f", FloatType()
                                                    ),
                                                    StructField(
                                                        "will_it_rain", BooleanType()
                                                    ),
                                                    StructField(
                                                        "chance_of_rain", FloatType()
                                                    ),
                                                    StructField(
                                                        "will_it_snow", BooleanType()
                                                    ),
                                                    StructField(
                                                        "chance_of_snow", FloatType()
                                                    ),
                                                    StructField("vis_km", FloatType()),
                                                    StructField(
                                                        "vis_miles", FloatType()
                                                    ),
                                                    StructField(
                                                        "gust_mph", FloatType()
                                                    ),
                                                    StructField(
                                                        "gust_kph", FloatType()
                                                    ),
                                                    StructField(
                                                        "condition",
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    "text", StringType()
                                                                ),
                                                                StructField(
                                                                    "icon", StringType()
                                                                ),
                                                                StructField(
                                                                    "code",
                                                                    IntegerType(),
                                                                ),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            )
                                        ),
                                    ),
                                ]
                            )
                        ),
                    )
                ]
            ),
        ),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC Para garantirmos que nosso schema acima é válido, vamos tentar criar um dataframe vazio para conferir se ele está de acordo com nossa definição

# COMMAND ----------

# Criando um Dataframe vazio para testar se nosso schema acima está definido corretamente 
try:
    dataframe = spark.createDataFrame([], schema = schema)
except:
    print("Error! There's an error on the provided schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definindo a UDF
# MAGIC Agora que nosso schema está definido corretamente, podemos definir a UDF que fará as requisições para obter os dados de clima

# COMMAND ----------

# Definindo a função que fará a requisição
@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=60)
def get_weather_data(url: str, host: str, key: str, city: str, date: str, language: str) -> dict:
    res = None
    headers = {
        "X-RapidAPI-Host": host,
        "X-RapidAPI-Key": key
    }
    query_string = {
        "q": city,
        "dt": date,
        "lang": language
    }
    print(query_string)
    # Make API request, get response object back, create dataframe from above schema.
    try:
        res = requests.request("GET", url=url, headers=headers, params=query_string)
    except Exception as e:
        return e

    if res != None and res.status_code == 200:
        return json.loads(res.text)

    return res

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definindo a amostra de dados que colheremos
# MAGIC Esta API de exemplo, só permite a leitura de dados climáticos de forma horária por até 7 dias para trás no modelo gratuito, então nosso exemplo puxará os dados para este período

# COMMAND ----------

from datetime import datetime, timedelta

final_date = datetime.now() - timedelta(days=1)
parameters = [{"city": "Sao_Paulo", "date": (final_date - timedelta(days=i)).strftime("%Y-%m-%d"), "language": "pt"} for i in range(8)]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Com todos os parâmetros prontos, montamos o nosso dataframe de requisição para trabalhar utilizar em conjunto com a UDF

# COMMAND ----------

columns = ["city", "date", "language"]
request_df = (
    spark.createDataFrame(data=parameters, schema=columns)
    .withColumn("verb", lit("get"))
    .withColumn("url", lit(url))
    .withColumn("X-RapidAPI-Host", lit("weatherapi-com.p.rapidapi.com"))
    .withColumn(
        "X-RapidAPI-Key",
        lit(dbutils.secrets.get("databricks_latam_demos", "weather_api_key")),
    )
)

# COMMAND ----------

request_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definindo a UDF:

# COMMAND ----------

udf_get_weather_data = udf(get_weather_data, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fazendo as requisições para todas as 7 linhas do dataframe através da UDF que definimos no passo anterior

# COMMAND ----------

# verb: str, url: str, headers: dict, body: dict, querystring: dict
request_df = (
                request_df.withColumn("result", udf_get_weather_data(
                                                                        col("url")
                                                                        , col("X-RapidAPI-Host")
                                                                        , col("X-RapidAPI-Key")
                                                                        , col("city")
                                                                        , col("date")
                                                                        , col("language")
                                                                    )
                                     )
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exibindo os resultados "crus"
# MAGIC Agora que temos os resultados das requisições, podemos tratar para obter apenas os campos que nos interessam, que, neste caso, são as informações sobre a localização que utilizamos para obter as informações e a previsão hora a hora para esta cidade (São Paulo-SP-Brasil)

# COMMAND ----------

request_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finalmente, extraímos apenas os campos que nos interessam e explodimos a previsão do tempo horária para diferentes linhas
# MAGIC Assim conseguimos ter acesso a linhas distintas para diferentes horas do dia

# COMMAND ----------

final_df = (
    request_df.withColumn("forecast_hourly", explode("result.forecast.forecastday"))
    .withColumn("forecast_hourly", explode("forecast_hourly.hour"))
    .select("result.location.*", "forecast_hourly.*")
    .select("*", "condition.text")
    .withColumnRenamed("text", "weather_condition")
    .drop("condition")
)

# COMMAND ----------

final_df.display()
