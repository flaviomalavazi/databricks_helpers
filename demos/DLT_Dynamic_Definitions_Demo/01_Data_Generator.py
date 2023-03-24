# Databricks notebook source
dbutils.widgets.text("nb_of_iterations", "1000", "How many iterations you want me to run?")
dbutils.widgets.text("path", "<LANDING_PATH>", "Where to put the data?")

# COMMAND ----------

from json import dumps
from datetime import datetime
from random import gauss, choice

class dataProducer():
    
    def __init__(self):
        self.collection = ""
        self.files = []
        
    def accumulate_records(self, measurement: dict) -> None:
        self.collection = self.collection + f"{dumps(measurement)}\n"

    def write_records_to_file(self, destination_path: str, file_name: str = None) -> None:
        if file_name == None:
            file_name = f"sensor_data_{datetime.now().strftime('%Y%m%dT%H%M%S%f')}.json"
        try:
            with open(f"{destination_path}/{file_name}", "w") as outfile:
                outfile.write(self.collection)
            self.collection = ""
        except Exception as e:
            if "No such file or directory:" in str(e):
                dbutils.fs.mkdirs(destination_path)
                self.write_records_to_file(file_name = file_name, destination_path = destination_path)
            else:
                raise e
        
    def generate_weather_message(self) -> dict:
        measurement = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f").strip()[:-2],
            "deviceId": "WeatherCapture",
            "temperature": gauss(28, 5),
            "humidity": gauss(50, 25),
            "windspeed": gauss(20,10),
            "winddirection": choice(["N", "NW", "W", "SW", "S", "NE", "SE", "E"]),
            "rpm": gauss(15,5),
            "angle": gauss(7,2),
        }
        return measurement

    def generate_turbine_message(self) -> dict:
        measurement = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f").strip()[:-2],
            "deviceId": choice(["WindTurbine-1","WindTurbine-2","WindTurbine-3","WindTurbine-4","WindTurbine-5","WindTurbine-6","WindTurbine-7","WindTurbine-8","WindTurbine-9","WindTurbine-10","WindTurbine-11"]),
            "rpm": gauss(15,5),
            "angle": gauss(7,2),
        }
        return measurement
    
    def generate_measurement(self, message_type: str) -> None:
        if message_type == "turbine":
            self.accumulate_records(measurement = self.generate_turbine_message())
        elif message_type == "weather":
            self.accumulate_records(measurement = self.generate_weather_message())
        else:
            pass


# COMMAND ----------

path = dbutils.widgets.get("path")

try:
    nb_of_interactions = int(dbutils.widgets.get("nb_of_iterations"))
except:
    nb_of_interactions = 1000

# COMMAND ----------

producer = dataProducer()

# COMMAND ----------

while (nb_of_interactions >= 0):
    turbine_measurements = 0
    while turbine_measurements <= 11:
        producer.generate_measurement(message_type="turbine")
        turbine_measurements = turbine_measurements + 1
    producer.generate_measurement(message_type="weather")
    producer.write_records_to_file(destination_path = path)
    nb_of_interactions = nb_of_interactions - 1