import requests
from bs4 import BeautifulSoup
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

bigquery_key = '/Users/User/tanner-project-588591097cf3.json'

website_url = requests.get("https://frozenpool.dobbersports.com/frozenpool_hotnot.php").text

soup = BeautifulSoup(website_url,"lxml")
#print(soup.prettify())


player_names = []

table=soup.find("tbody")
#print(table)
table_names = table.find_all("a", href=True)
#print(table_names)

for tr in table_names:
	playername = tr.text
	#print(playername)
	playername = playername.lower()
	playername = player_names.append(playername.title())
	

df1 = pd.DataFrame(player_names)
df1['HotPlayer'] = 1
df1 = df1.replace(r'\t',' ', regex=True) 
df1 = df1.replace(r'\n',' ', regex=True)
df1.columns = ['PlayerName','HotPlayer']
#print(df1.head())


df1.to_gbq(
	'hockey.players_streaking',
	'dulcet-outlook-227105',
	chunksize=5000,
	verbose=True,
	reauth=False,
	if_exists='replace',
	private_key = bigquery_key
	)
