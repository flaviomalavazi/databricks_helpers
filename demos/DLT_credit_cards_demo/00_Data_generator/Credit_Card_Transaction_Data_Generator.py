# Databricks notebook source
# MAGIC %sh
# MAGIC pip install Faker --quiet --disable-pip-version-check

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("nb_of_iterations", "1000", "How many iterations you want me to run?")
dbutils.widgets.text("path", "dbfs:/Users/<SEU_EMAIL>/demos/dlt_credit_cards", "Where to put the data?")

# COMMAND ----------

restaurants = [
    "Taste it Restaurante", "V.E.R.A Restaurante", "Trencadís Restaurante", "Nagarê Sushi", "L'entrecote de Paris",
    "Seen São Paulo", "Allora Vino e Birra", "Pizzaria Napoli Centrale", "Temakeria e Cia Pinheiros", "Niks Bar",
    "Sushi Papaia", "Brown Sugar Restaurante", "Zendô Sushi", "Qceviche", "Bar Paribar", "Poke Me Up",
    "Latitude 12 SP Bar e Restaurante", "Smart Burger", "Fiori d'Itália", "Riqs Burguer Shop", "Japa 25",
    "Wanderlust Bar", "O Holanddês Hamburgueria", "Acarajé e Delícias da Bahia", "Doña Luz Empanadas", "Da Vinci Burger",
    "Bawarchi", "Raw Veggie Burger", "Rosso Burguer", "Itsu Restaurante", "Parm", "Forneria Sant'Antonio", "Rudá Restaurante",
    "Worst Burguer", "Soggiorno da Vila", "Pecorino", "Rueiro Parrilla Bar", "Harumi Tatuapé", "Restaurante Banri", "Poke Haus",
    "Grecco Bar e Cucina", "Hunter We Burger", "We Pizza", "Ydaygorô Sushi", "Casale di Pizza", "The Choice Hamburgueria",
    "BBOPQ Grill", "2nd House Bar", "Mama África La Bonne Bouffe", "Pasta Nostra Ristorante", "Mais Burguinho",
    "Bem Torta Bistro & Lounge", "BBC Burger & Crepe", "LIL' Square", "La Forchetta", "Di Bari Pizza",
    "Nacho Libre", "Di Fondi Pizza", "Galeto di Paolo", "Kalili Restaurante", "Pacífico Taco Shop", "La Innocent Bistrô & Bar",
    "Figone", "1101 bar", "Le Burger Hamburgueria", "The Pitchers Burger Baseball", "Forno da Vila", "Montemerano Pizzeria",
    "Oriba Restaurante", "Café Kidoairaku", "The Bowl", "Peixaria Império dos Pescados", "Reino do Churrasco", "Lá da Torta",
    "Projeto Burguer Vila Burguer", "Espeto de la Vaca", "O Kebab SP", "Low BBQ", "Posto 9", "Craft Burguer", "BnB Hamburgueria",
    "North Beer", "Burger Match", "Ursus Rock Burger", "Restaurante Shisá", "Let's Poke", "Hamburgueria Invictus",
    "Twins Pizza & Burger", "Bistro Bar Vila Olímpia", "Mussanela Pizzaria", "Santô Sushi", "Basic Burger",
    "Restaurante Migá", "Vila Milagro", "Burger 700", "Flying Sushi", "Bologna", "Pizzaria Copan", "FiChips", "Taco Bell",
]

retail = [
    "Mercado Livre", "B2W Americanas", "Amazon", "Via Varejo", "Magalu", "OLX", "Shopee", "Carrefour",
    "Global Fashion Group (GFG)", "Submarino", "Elo7", "Enjoei", "Shopfácil", "Centauro", "Netshoes", "MadeiraMadeira",
    "Wish", "Estante Virtual", "Atacado.com", "2Collab", "Boobam", "Infoar/Webcontinental", "Alluagro",
    "Loja Integrada", "Canal da Peça"
]

drugstore_and_aesthetics = [
    "Grupo Boticário", "RaiaDrogasil", "Grupo DPSP", "Farmácias Pague Menos", "Farmácias Associadas",
    "Farmácias São João", "Natura&Co", "Clamed Farmácias/Drogaria Caterinense", "Panvel Farmácias", "Extrafarma"
]

movies = [
    "A2 Filmes", "AFA Cinemas", "Alpha Filmes", "Arcoplex Cinemas", "Box Cinemas", "Bretz Filmes", "California Filmes", "Cavídeo",
    "Centerplex", "Cine A", "Cine Filmes", "Cine Gracher", "Cineart", "Cineflix", "Cinema Virtual", "Cinemagic",
    "Cinemais", "Cinemark", "Cinemas Premier", "Cinematográfica Araújo", "Cinemaxx", "Cineplus", "Cinépolis",
    "Cinespaço", "Cinesystem", "Circuito Entretenimento e Cinemas", "Copacabana Filmes", "Diamond Films", "Downtown Filmes",
    "Elite Filmes", "Embracine", "Empresa de Cinemas Sercla", "Esfera Filmes", "Espaço Filmes", "Espaço Itaú de Cinema", "Europa Filmes",
    "FamDVD", "Fênix Filmes", "Filmicca", "Galeria Distribuidora", "GNC Cinemas", "Grupo Cine", "Grupo Estação",
    "Grupo Mobi Cine", "Grupo SaladeArte", "H2O Films", "Imagem Filmes", "Imovision", "Kinoplex", "Laser Cinemas",
    "Lume Filmes", "Lumière Brasil", "Lumière Empresa Cinematográfica", "Mares Filmes", "Moviecom", "Moviemax", "Multicine",
    "Nossa Distribuidora", "Orient Cinemas", "Pagu Pictures", "Pandora Filmes", "Paris Filmes", "Petra Belas Artes", "Pinheiro Cinemas",
    "Rede Cine Show", "RioFilme", "Roxy Cinemas", "Sofa Digital", "UCI Kinoplex", "UCI Orient", "Uniplex", "United Cinemas International",
    "Versátil Home Vídeo", "Videocamp", "VideoFilmes", "Vitrine Filmes", "Zeta Filmes"
]


segments = {
    "restaurants": restaurants,
    "retail": retail,
    "drugstore_and_aesthetics": drugstore_and_aesthetics,
    "movies": movies
}


# COMMAND ----------

from datetime import datetime, timedelta
from json import dumps
from random import choice, gauss, randint
from uuid import uuid4

from faker import Faker


class dataProducer():
    def __init__(self, **kwargs):
        self.collection = ""
        self.files = []
        self.parameter_dict = kwargs.get("parameter_dict", {"key": []})
        self.debug = kwargs.get("debug", False)
        self.verbose = kwargs.get("verbose", False)
        self.card_networks = ["Visa", "Mastercard", "Amex", "Elo"]
        self.legitimate_card_bins = {
            "Mastercard": [51, 52, 53, 54],
            "Visa": [4],
            "Amex": [34, 37],
            "Elo": [636368, 636369, 438935, 504175, 451416, 636297, 5067, 4576, 4011, 506699]
        }
        self.fake = Faker()

    def accumulate_records(self, measurement: dict) -> None:
        self.collection = self.collection + f"{dumps(measurement, allow_nan=True)}\n"

    def write_records_to_file(self, destination_path: str, file_name: str = None) -> None:
        if file_name is None:
            file_name = f"data_{datetime.now().strftime('%Y%m%dT%H%M%S%f')}.json"
        try:
            if self.debug:
                print(self.collection)
            else:
                dbutils.fs.put(f"{destination_path}/{file_name}", self.collection, True)
            self.collection = ""
        except Exception as e:
            if "No such file or directory:" in str(e):
                dbutils.fs.mkdirs(destination_path)
                self.write_records_to_file(file_name=file_name, destination_path=destination_path)
            else:
                raise e

    def generate_bill_multiplier(self, card_network: str, merchant_type: str) -> float:
        merchant_offset = 1.0
        card_network_offset = 1.0
        match card_network:
            case "Mastercard":
                card_network_offset = 0.95
            case "Visa":
                card_network_offset = 1
            case "Amex":
                card_network_offset = 1.25
            case "Elo":
                card_network_offset = 0.9
        match merchant_type:
            case "restaurants":
                merchant_offset = 1.4
            case "retail":
                merchant_offset = 2
            case "drugstore_and_aesthetics":
                merchant_offset = 1
            case "movies":
                merchant_offset = 0.8

        return merchant_offset * card_network_offset

    def generate_legitimate_transaction(self, parameter_dict: dict) -> dict:
        card_network = choice(self.card_networks)
        merchant_type = choice(list(self.parameter_dict.keys()))
        if self.debug:
            print(f"card_network = {card_network}, merchant_type = {merchant_type}")
        measurement = {
            "transaction_id": f"{uuid4()}",
            "timestamp": self.generate_timestamp().strip()[:-2],
            "merchant_name": choice(parameter_dict[merchant_type]),
            "merchant_type": merchant_type,
            "bill_value": (gauss(100, 50) + randint(-20, 20)) * self.generate_bill_multiplier(card_network=card_network, merchant_type=merchant_type),
            "installments": choice([1, 1, 1, 1, 1, 1, 1, 2, 3]),
            "card_network": card_network,
            "card_bin": choice(self.legitimate_card_bins[card_network]),
            "card_holder": self.fake.name(),
            "card_expiration_date": self.fake.credit_card_expire(),
            "currency": self.fake.currency_code(),
            "type": "expense"
        }
        return measurement

    def generate_timestamp(self) -> datetime.timestamp:
        date = (datetime.now().date() + timedelta(days=randint(-120, -61))).strftime("%Y-%m-%d")
        date_time = f"""{date} {self.generate_hour_of_day()}:{str(datetime.now().minute).zfill(2)}:{str(datetime.now().second).zfill(2)}.{str(datetime.now().microsecond).zfill(2)}"""
        return date_time

    def generate_hour_of_day(self):
        return str(choice([
            0, 0, 0, 0,
            1, 1, 1,
            2, 2,
            3, 3,
            4, 4,
            5, 5,
            6, 6, 6,
            7, 7, 7, 7,
            8, 8, 8, 8, 8,
            9, 9, 9, 9, 9, 9,
            10, 10, 10, 10, 10, 10, 10,
            11, 11, 11, 11, 11, 11, 11, 11,
            12, 12, 12, 12, 12, 12, 12, 12, 12,
            13, 13, 13, 13, 13, 13, 13, 13, 13,
            14, 14, 14, 14, 14, 14, 14,
            15, 15, 15, 15, 15, 15,
            16, 16, 16, 16, 16,
            17, 17, 17, 17, 17, 17, 17,
            18, 18, 18, 18, 18, 18, 18, 18, 18,
            19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19,
            20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
            21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21,
            22, 22, 22, 22, 22, 22,
            23, 23, 23, 23, 23
        ])).zfill(2)

    def generate_fauty_transaction(self, parameter_dict: dict) -> dict:
        base_measurement = self.generate_legitimate_transaction(parameter_dict=parameter_dict)
        measurement = {}
        feature_to_break = choice(["timestamp", "merchant_name", "bill_value", "installments", "card_network", "card_bin", "card_holder"])
        if ((feature_to_break == "timestamp")):
            measurement["timestamp"] = (datetime.now() + timedelta(hours=randint(1, 5), minutes=randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S.%f").strip()[:-2]
        elif ((feature_to_break == "merchant_name")):
            measurement["merchant_name"] = None
        elif ((feature_to_break == "bill_value")):
            measurement["bill_value"] = - base_measurement["bill_value"]
        elif ((feature_to_break == "installments")):
            measurement["installments"] = 0
        elif ((feature_to_break == "card_network")):
            measurement["card_network"] = "UCB"
        elif ((feature_to_break == "card_bin")):
            measurement["card_bin"] = choice(self.legitimate_card_bins[choice(self.card_networks)])
        elif ((feature_to_break == "card_holder")):
            measurement["card_holder"] = None
        prio_dict = {1: base_measurement, 2: measurement}
        final_measurement = {**prio_dict[1], **prio_dict[2]}
        if self.debug:
            measurement["faulty_transaction"] = True
            if self.verbose:
                print(f"Broken measure to merge: {measurement}")
                print(f"Final Measurement: {final_measurement}")
        return final_measurement

    def generate_chargeback_transaction(self, parameter_dict: dict) -> dict:
        measurement = self.generate_legitimate_transaction(parameter_dict=parameter_dict)
        measurement["type"] = "chargeback"
        measurement["bill_value"] = - measurement["bill_value"]
        return measurement

    def generate_measurement(self, parameter_dict: dict = {"key": []}, message_type: str = None) -> None:
        message_type = choice(["legitimate", "legitimate", "legitimate", "legitimate", "chargeback", "chargeback", "faulty"]) if message_type is None else message_type
        parameter_dict = parameter_dict if parameter_dict != {"key": []} else self.parameter_dict
        if ((message_type == "legitimate") and (parameter_dict != {"key": []})):
            self.accumulate_records(measurement=self.generate_legitimate_transaction(parameter_dict=self.parameter_dict))
        elif ((message_type == "chargeback") and (parameter_dict != {"key": []})):
            self.accumulate_records(measurement=self.generate_chargeback_transaction(parameter_dict=self.parameter_dict))
        elif ((message_type == "faulty") and (parameter_dict != {"key": []})):
            self.accumulate_records(measurement=self.generate_fauty_transaction(parameter_dict=self.parameter_dict))
        else:
            pass

# COMMAND ----------


path = dbutils.widgets.get("path")
try:
    nb_of_interactions = int(dbutils.widgets.get("nb_of_iterations"))
except Exception as e:
    print(e)
    nb_of_interactions = 1000

# COMMAND ----------

producer = dataProducer(parameter_dict=segments, debug=False)

# COMMAND ----------

while (nb_of_interactions > 0):
    buffer_messages = 0
    while buffer_messages <= 50:
        producer.generate_measurement()
        buffer_messages = buffer_messages + 1
    producer.write_records_to_file(destination_path=path)
    nb_of_interactions = nb_of_interactions - 1
