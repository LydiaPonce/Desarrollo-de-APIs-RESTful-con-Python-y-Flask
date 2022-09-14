
from flask import Flask
import pandas as pd
from flask import jsonify

app = Flask(__name__)


@app.route('/hotels/list', methods=["GET"]) 
def list_hotels():
    hotels = pd.read_csv('prices.csv')
    list_hotel=list(hotels.hotel.sample(50))
    result= {'hotels':list_hotel}
    return jsonify(result)


@app.route('/hotels/data', methods=["POST"]) 
def data_hotels():

    { "hotels":
			["Hotel 1", "Hotel 2", "Hotel 3"], 
	  "dateFrom": "2022-01-01", 
	  "dateTo": "2022-05-30" }

def DailyMinPrices(): 
    result 

    