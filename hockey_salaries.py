import requests
from bs4 import BeautifulSoup
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


bigquery_key = '/Users/User/tanner-project-588591097cf3.json'

def players ():
	
	website_url = requests.get("http://www.numberfire.com/nhl/daily-fantasy/daily-hockey-projections").text

	soup = BeautifulSoup(website_url,"lxml")
	#print(soup.prettify())

	player_names = []

	#table = soup.find("")

	table=soup.find("tbody")
	table_names = table.find_all("a",{"class":"full"})
	#print(table_names)
	#print(table)

	for tr in table_names:
		playername = tr.text
		#print(playername)
		#row = [tr.text for tr in table_names]
		#print(row)
		player_names.append(playername)

	df1 = pd.DataFrame(player_names)
	df1 = df1.replace(r'\t',' ', regex=True) 
	df1 = df1.replace(r'\n',' ', regex=True)
	#print(df1.head())



	#######################
	# BEGIN Player Salaries 
	#######################

	player_salaries = []


	table_salary = table.find_all("td",{"class":"cost"})
	#print(table_salary)
	#print(table)

	for tr in table_salary:
		playersalary = tr.text
		#print(playersalary)
		player_salaries.append(playersalary)

	df2 = pd.DataFrame(player_salaries)
	df2 = df2.replace(r'\t',' ', regex=True) 
	df2 = df2.replace(r'\n',' ', regex=True)

	#print(df2.head())
		
	df3 = pd.concat([df1, df2], axis=1)
	df3.columns= ['PlayerName', 'Salary']
	df3['PlayerName'] = df3['PlayerName'].apply(lambda x: x.strip())
	df3['Salary'] = df3['Salary'].str.replace(r'$','')
	df3['Salary'] = df3['Salary'].str.replace(r',','')
	#df3['Salary'] = df3['Salary'].str.replace(r'N/A','0')
	df3['Date'] = pd.Timestamp("today").strftime("%m/%d/%Y")
	
	#print(df3)


	df3.to_gbq(
		'hockey.player_salaries',
		'dulcet-outlook-227105',
		chunksize=5000,
		verbose=True,
		reauth=False,
		if_exists='replace',
		private_key = bigquery_key
		)
	df3.to_gbq(
		'hockey.player_salaries_history',
		'dulcet-outlook-227105',
		chunksize=5000,
		verbose=True,
		reauth=False,
		if_exists='append',
		private_key = bigquery_key
		)

def goalies():

	website_url = requests.get("http://www.numberfire.com/nhl/daily-fantasy/daily-hockey-projections/goalies").text

	soup = BeautifulSoup(website_url,"lxml")
	#print(soup.prettify())

	table=soup.find("tbody")
	table_names = table.find_all("a",{"class":"full"})
	#print(table_names)


	player_names = []

	for tr in table_names:
		playername = tr.text
		#print(playername)
		#row = [tr.text for tr in table_names]
		#print(row)
		player_names.append(playername)

	df1 = pd.DataFrame(player_names)
	df1 = df1.replace(r'\t',' ', regex=True) 
	df1 = df1.replace(r'\n',' ', regex=True)
	#print(df1.head())



	#######################
	# BEGIN Goalie Salaries 
	#######################

	player_salaries = []


	table_salary = table.find_all("td",{"class":"cost"})

	#print(table_salary)
	#print(table)

	for tr in table_salary:
		playersalary = tr.text
		#print(playersalary)
		player_salaries.append(playersalary)


	df2 = pd.DataFrame(player_salaries)
	df2 = df2.replace(r'\t',' ', regex=True) 
	df2 = df2.replace(r'\n',' ', regex=True)

	#############################
	# BEGIN Goalie FanDuel Points 
	#############################

	player_points = []

	table_points = table.find_all("td",{"class":"fp"})
	#print(table_points)

	for tr in table_points:
		playerpoints = tr.text
		#print(playersalary)
		player_points.append(playerpoints)
		


	df3 = pd.DataFrame(player_points)
	df3 = df3.replace(r'\t',' ', regex=True) 
	df3 = df3.replace(r'\n',' ', regex=True)

	#print(df3.head())
		
		
	df4 = pd.concat([df1, df2, df3], axis=1)
	df4.columns= ['PlayerName', 'Salary', 'Points']
	df4['PlayerName'] = df4['PlayerName'].apply(lambda x: x.strip())
	df4['Salary'] = df4['Salary'].str.replace(r'$','')
	df4['Salary'] = df4['Salary'].str.replace(r',','')
	df4['Date'] = pd.Timestamp("today").strftime("%m/%d/%Y")
	#df3['Salary'] = df3['Salary'].str.replace(r'N/A','0')

	#print(df4)


	df4.to_gbq(
		'hockey.goalie_salaries',
		'dulcet-outlook-227105',
		chunksize=5000,
		verbose=True,
		reauth=False,
		if_exists='replace',
		private_key = bigquery_key
		)
	df4.to_gbq(
		'hockey.goalie_salaries_history',
		'dulcet-outlook-227105',
		chunksize=5000,
		verbose=True,
		reauth=False,
		if_exists='append',
		private_key = bigquery_key
		)


players()
goalies()
	
