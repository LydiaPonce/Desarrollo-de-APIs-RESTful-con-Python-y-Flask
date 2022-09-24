
from socket import fromfd
from urllib import response
from flask import Flask
import pandas as pd
from flask import jsonify
from flask import request
import random
import json
from datetime import datetime
from flask import Response

app = Flask(__name__)


# 1: Data Read - API (Get)
# =========================

@app.route('/hotels/list', methods=["GET"]) 
def list_hotels():
    hotels = pd.read_csv('prices.csv')
    list_hotel=list(hotels.hotel.sample(50))
    result= {'hotels':list_hotel}
    return jsonify(result)



# 2: Data Read - API (Post)
# =========================

@app.route('/hotels/data', methods=["POST"]) 
def data_hotels():
    #Obtener Json de llamada
    data = request.get_json() 
    #Obtener los parametros
    hotels = data['hotels'] #hotel 1, hotel 2, hotel 3
    dateFrom = data['dateFrom']
    dateTo = data['dateTo']
    
    #Definimos listas de fechas
    date = pd.date_range(dateFrom,dateTo).strftime('%Y-%m-%d').tolist() 
    list_prices = generate_list_prices(hotels,date)
    
    
    dailyMinPrices = []
    dailyMaxPrices = []
    dailyAvgPrices = []

    #Para obtener precios MIN y MAX
    for d in date: 
        #Aqui estan los 15 precios del día (5 por cada hotel)
        list_day = list(filter(lambda x: (x.date == d), list_prices))
        for hotel in hotels: 
            #Aquí estan los hoteles por día (hotel 1, hotel 2, hotel 3)
            list_day_hotel = list(filter(lambda x: (x.hotel == hotel, list_day)))
            dailyMinPrices.append(min(list_day_hotel, key=lambda x:x.price))
            dailyMaxPrices.append(max(list_day_hotel, key=lambda x:x.price))
            
    #Para la AVG
            sum_price = 0
            count= 0
            for p in list_day_hotel:
                sum_prices= p.price
                count +=1
            avg_price = sum_price/count
            dailyAvgPrices.append(Price(avg_price,hotel,d))

    #MoreExpensiveHotel
    expensivePrice = max(dailyAvgPrices,key=lambda x:x.price)
    moreExpensiveHotel = expensivePrice.getHotel()

    #MoreCheapestHotel
    cheapestPrice = min(dailyAvgPrices,key=lambda x:x.price)
    moreCheapestHotel = cheapestPrice.getHotel()

    # MoreExpensiveDay 
    expensiveDay= max(dailyMaxPrices,key=lambda x:x.price)
    moreExpensiveDay = expensiveDay.getDate()

    #Pasamos a JSON (convierte a diccionario todas las propiedades de cada elemento)
    dailyMinPrices = json.dumps(dailyMinPrices, default = lambda x: x.__dict__,indent=1)
    dailyMaxPrices = json.dumps(dailyMaxPrices, default = lambda x: x.__dict__,indent=1)
    dailyAvgPrices = json.dumps(dailyAvgPrices, default = lambda x: x.__dict__,indent=1)
   
    # Por último debería poder devolver un JSON con toda la información que requerimos.
    js = {"DailyMinPrices":json.loads(dailyMinPrices),"DailyMaxPrices":json.loads(dailyMaxPrices),"DailyAvgPrices":json.loads(dailyAvgPrices),
    "MoreExpensiveHotel":moreExpensiveHotel,"CheapestHotel":moreCheapestHotel,"MoreExpensiveDay":moreExpensiveDay}

    return js

    
# Genera una lista de la clase Price, 5 precios random para cada Hotel y cada fecha.
def generate_list_prices(hotels,dates): 
    prices = []

    for hotel in hotels:
        for date in dates:
            for i in range(0,5):
                prices.append(Price(random_price(),hotel,date))

    return prices


# Elegir precios ramdon entre 50 y 200 
def random_price(): 
    return random.randint(50,200)


# Estructura de un precio (estructura ejemplo)
class Price(): 
    price = ""
    hotel = ""
    date = ""
    board = ""
    room = ""
    def __init__(self,price,name,date):
        self.price = price
        self.hotel = name
        self.date = date
        self.board = "All inclusive"
        self.room = "Double"

    def getPrice(self):
        return self.price

    def getHotel(self):
        return self.hotel

    def getDate(self):
        return self.date
    


# 3: Data Ingestion
# ===================






# 4: Data Storage
# ================

import csv
import sqlite3

#Creamos variables para trabajar con la BBDD
conexion = sqlite3.connect("Prices.db")
consulta = conexion.cursor()

#Creamos la tabla en la BD
sql = """CREATE TABLE IF NOT EXISTS prices(
board VARCHAR (20) NOT NULL,
date VARCHAR (20) NOT NULL,
hotel VARCHAR (20) NOT NULL,
price INTEGER (5) NOT NULL,
rate INTEGER (5) NOT NULL,
room VARCHAR (20) NOT NULL
)
"""

#Ejecutamos la creación de la tabla
if (consulta.execute(sql)):
    print("Table successfully created")
else:
    print("Error")


@app.route('/bd', methods=["GET"])
def BD_csv():
    # Insertamos la info de csv en BD
    with open ('prices.csv','r') as file:
        records= 0
    for row in file:
        consulta.execute("INSERT INTO prices VALUES (?,?,?,?,?,?)",row.split(","))
        records +=1
    conexion.close()
    print('\n{} Records Transfer Completed'.format(records))
