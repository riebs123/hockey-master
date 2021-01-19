import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account



bigquery_key = service_account.Credentials.from_service_account_file('/Users/Tanner/Desktop/hockey-gbq.json')


def advanced_stats ():
	
	data = pd.read_csv("http://moneypuck.com/moneypuck/playerData/seasonSummary/2020/regular/skaters.csv")
	print(data.head())
	df1 = pd.DataFrame(data)

	df1.to_gbq(
		'hockey_test.players_season_stats_advanced',
		'dulcet-outlook-227105',
		chunksize=5000,
		if_exists='replace',
		reauth=False,
		credentials = bigquery_key
		)
		

advanced_stats()

