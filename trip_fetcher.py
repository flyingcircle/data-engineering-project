from bs4 import BeautifulSoup as bs
import numpy as np
import pandas as pd
import requests
import regex as re
import json
from datetime import date

 

def get_data():
	page = requests.get("http://www.psudataeng.com:8000/getStopEvents/")

	if page.status_code == 200:
		soup = bs(page.content, "html.parser")
		trips_raw = soup.find_all('h3')
		tables_raw = soup.find_all('table')	
		route_nums = []
		service_keys = []
		directions = []

		for table in tables_raw:
			rnum = table.find_all('tr')[1].contents[7]
			dire = table.find_all('tr')[1].contents[9]
			skey = table.find_all('tr')[1].contents[11]
			route_nums.append(int(rnum.get_text()) if rnum.get_text() else np.nan)
			directions.append(int(dire.get_text()) if dire.get_text() else np.nan)
			service_keys.append(skey.get_text() if skey.get_text()  else str(np.nan))

		id_re = re.compile(r'\d{9}')
		trip_ids = [int(id_re.search(trip.get_text()).group()) for trip in trips_raw]	

		print(len(trip_ids))
		print(len(route_nums))
		print(len(directions))
		print(len(service_keys))

		trips = pd.DataFrame({
			"trip_id": trip_ids,
			"route_num": route_nums,
			"direction": directions,
			"service_key": service_keys, 
		})
		res = trips.to_json(orient="records")
		parsed = json.loads(res)
		print(parsed[300])
		out_file = open("/home/production/data/" + str(date.today())+"-stopevent.json", "w")
		json.dump(parsed, out_file)
		return parsed

if __name__ == "__main__":
	 get_data()