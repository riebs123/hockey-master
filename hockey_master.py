import requests
import json
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

bigquery_key = service_account.Credentials.from_service_account_file('/Users/Tanner/Desktop/hockey-gbq.json')

#####################
# GET TODAYS SCHEDULE
#####################

schedule_url = 'https://statsapi.web.nhl.com/api/v1/schedule?date=2021-01-18'
schedule_response = requests.get(schedule_url)
schedule_parse = json.loads(schedule_response.text) 

todays_team_array = []

for x in schedule_parse['dates']:
	for y in x['games']:
		todays_team_array.append([
			y['teams']['away']['team']['id']
			])
	for y in x['games']:
		todays_team_array.append([
			y['teams']['home']['team']['id']
			])

#########################
# GET TEAMS & CONFERENCES
#########################

team_url = 'http://statsapi.web.nhl.com/api/v1/teams'
team_response = requests.get(team_url)
team_parse = json.loads(team_response.text)

team_array = []
dfteams = pd.DataFrame(todays_team_array)

flattened = [val for sublist in todays_team_array for val in sublist]

for x in team_parse['teams']:
		if (x['id']) in flattened: #Filters for today's games
			team_array.append([
				x['name'],
				x['id'],
				x['division']['name'],
				x['division']['id'],
				x['conference']['name'],
				x['conference']['id']
				])

######################################################
# BEGIN GETTING ROSTERS (Player Name, ID and Position)
######################################################

player_array = []

for a, b, c, d, e, f in team_array:
	roster_url = 'http://statsapi.web.nhl.com/api/v1/teams/'+str(b)+'/roster'
	roster_response = requests.get(roster_url)
	#print(roster_response)
	roster_parse = json.loads(roster_response.text)

	for x in roster_parse['roster']:
		player_array.append([
			a,b,c,d,e,f,
			x['person']['fullName'],
			x['person']['id'],
			x['position']['name']
			])

def get_sec(time_str):
	m, s = time_str.split(':')
	return int(m) * 60 + int(s)
	
def stats():

	##############################################
	# BEGIN GETTING STATS (Baseline Season Stats )
	##############################################
	player_stats_array = []
	goalie_stats_array = []

	for a, b, c, d, e, f, g, h, i in player_array:
		stats_url = 'https://statsapi.web.nhl.com/api/v1/people/'+str(h)+'/stats/?stats=statsSingleSeason'
		stats_response = requests.get(stats_url)
		stats_parse = json.loads(stats_response.text)
		for x in stats_parse['stats']:
			for y in x['splits']:
				if i == 'Goalie':
					goalie_stats_array.append([
						a,b,c,d,e,f,g,h,i,
						y['stat']['ot'],
						y['stat']['shutouts'],		
						y['stat']['ties'],
						y['stat']['wins'],
						y['stat']['losses'],
						y['stat']['saves'],
						y['stat']['powerPlaySaves'],
						y['stat']['shortHandedSaves'],
						y['stat']['evenSaves'],
						y['stat']['shortHandedShots'],
						y['stat']['evenShots'],
						y['stat']['powerPlayShots'],
						y['stat']['savePercentage'],
						y['stat']['goalAgainstAverage'],
						y['stat']['games'],
						y['stat']['gamesStarted'],
						y['stat']['shotsAgainst'],
						y['stat']['goalsAgainst'],
						y['stat']['timeOnIcePerGame'],
						y['stat']['evenStrengthSavePercentage']
					])
				else:
					player_stats_array.append([
						a,b,c,d,e,f,g,h,i,
						get_sec(y['stat']['timeOnIce']),
						y['stat']['assists'],		
						y['stat']['goals'],
						y['stat']['pim'],
						y['stat']['shots'],
						y['stat']['hits'],
						y['stat']['games'],
						y['stat']['powerPlayGoals'],
						y['stat']['powerPlayPoints'],
						get_sec(y['stat']['powerPlayTimeOnIce']),
						get_sec(y['stat']['evenTimeOnIce']),
						y['stat']['faceOffPct'],
						y['stat']['shotPct'],
						y['stat']['gameWinningGoals'],
						y['stat']['overTimeGoals'],
						y['stat']['shortHandedGoals'],
						y['stat']['shortHandedPoints'],
						get_sec(y['stat']['shortHandedTimeOnIce']),
						y['stat']['blocked'],
						y['stat']['plusMinus'],
						y['stat']['shifts'],
						y['stat']['points'],
						get_sec(y['stat']['timeOnIcePerGame']),
						get_sec(y['stat']['evenTimeOnIcePerGame']),
						get_sec(y['stat']['shortHandedTimeOnIcePerGame']),
						get_sec(y['stat']['powerPlayTimeOnIcePerGame'])

					])

	df1 = pd.DataFrame(player_stats_array)

	df1.columns= ['TeamName', 'TeamId', 'DivisionName', 'DivisionId', 'ConferenceName', 'ConferenceId',
				'PlayerName', 'PlayerId', 'Position', 'timeOnIce', 'assists', 'goals','pim', 'shots', 'hits',
				'games', 'powerPlayGoals', 'powerPlayPoints', 'powerPlayTimeOnIce', 'evenTimeOnIce',
				'faceOffPct', 'shotPct', 'gameWinningGoals', 'overTimeGoals', 'shortHandedGoals', 'shortHandedPoints',
				'shortHandedTimeOnIce', 'blocked', 'plusMinus', 'shifts', 'points', 'timeOnIcePerGame',
				'evenTimeOnIcePerGame', 'shortHandedTimeOnIcePerGame', 'powerPlayTimeOnIcePerGame']

	df1['fanduelpoints']= (df1['goals']*12)+(df1['assists']*8)+(df1['powerPlayGoals']*0.5)+(df1['shortHandedGoals']*2)+(df1['shots']*1.60)+(df1['blocked']*1.6)

	#df2 = pd.DataFrame(goalie_stats_array)

	df1.to_gbq(
		'hockey_test.players_season_stats',
		'dulcet-outlook-227105',
		chunksize=5000,
		if_exists='replace',
		reauth=False,
		credentials = bigquery_key
		)

def gamelogs():

	ten_player_log = []
	ten_goalie_log = []

	for a, b, c, d, e, f, g, h, i in player_array:
		stat_log_url = 'https://statsapi.web.nhl.com/api/v1/people/'+str(h)+'/stats/?stats=gameLog'
		stat_log_response = requests.get(stat_log_url)
		stat_log_parse = json.loads(stat_log_response.text)

		for x in stat_log_parse['stats']:
			for y in x['splits'][:10]:
				if i == 'Goalie':
					ten_goalie_log.append([					
						a,b,c,d,e,f,g,h,i])
				else:
					ten_player_log.append([
						a,b,c,d,e,f,g,h,i,
						get_sec(y['stat']['timeOnIce']),
						y['stat']['assists'],		
						y['stat']['goals'],
						y['stat']['pim'],
						y['stat']['shots'],
						y['stat']['hits'],
						y['stat']['games'],
						y['stat']['powerPlayGoals'],
						y['stat']['powerPlayPoints'],
						get_sec(y['stat']['powerPlayTimeOnIce']),
						get_sec(y['stat']['evenTimeOnIce']),
						#y['stat']['shotPct'],
						y['stat']['gameWinningGoals'],
						y['stat']['overTimeGoals'],
						y['stat']['shortHandedGoals'],
						y['stat']['shortHandedPoints'],
						get_sec(y['stat']['shortHandedTimeOnIce']),
						y['stat']['blocked'],
						y['stat']['plusMinus'],
						y['stat']['shifts'],
						y['stat']['points'],
						])


	df1 = pd.DataFrame(ten_player_log)
	df1.columns= ['TeamName', 'TeamId', 'DivisionName', 'DivisionId', 'ConferenceName', 'ConferenceId',
				'PlayerName', 'PlayerId', 'Position', 'tentimeOnIce', 'tenassists', 'tengoals','tenpim', 'tenshots', 'tenhits',
				'tengames', 'tenpowerPlayGoals', 'tenpowerPlayPoints', 'tenpowerPlayTimeOnIce', 'tenevenTimeOnIce',
				'tengameWinningGoals', 'tenoverTimeGoals', 'tenshortHandedGoals', 'tenshortHandedPoints',
				'tenshortHandedTimeOnIce', 'tenblocked', 'tenplusMinus', 'tenshifts', 'tenpoints']


	five_player_log = []
	five_goalie_log = []


	for a, b, c, d, e, f, g, h, i in player_array:
		stat_log_url = 'https://statsapi.web.nhl.com/api/v1/people/'+str(h)+'/stats/?stats=gameLog'
		stat_log_response = requests.get(stat_log_url)
		stat_log_parse = json.loads(stat_log_response.text)

		for x in stat_log_parse['stats']:
			for y in x['splits'][:5]:
				if i == 'Goalie':
					five_goalie_log.append([					
						a,b,c,d,e,f,g,h,i])
				else:
					five_player_log.append([
						a,b,c,d,e,f,g,h,i,
						get_sec(y['stat']['timeOnIce']),
						y['stat']['assists'],		
						y['stat']['goals'],
						y['stat']['pim'],
						y['stat']['shots'],
						y['stat']['hits'],
						y['stat']['games'],
						y['stat']['powerPlayGoals'],
						y['stat']['powerPlayPoints'],
						get_sec(y['stat']['powerPlayTimeOnIce']),
						get_sec(y['stat']['evenTimeOnIce']),
						#y['stat']['shotPct'],
						y['stat']['gameWinningGoals'],
						y['stat']['overTimeGoals'],
						y['stat']['shortHandedGoals'],
						y['stat']['shortHandedPoints'],
						get_sec(y['stat']['shortHandedTimeOnIce']),
						y['stat']['blocked'],
						y['stat']['plusMinus'],
						y['stat']['shifts'],
						y['stat']['points'],
						])

	df2 = pd.DataFrame(five_player_log)
	df2.columns= ['TeamName', 'TeamId', 'DivisionName', 'DivisionId', 'ConferenceName', 'ConferenceId',
				'PlayerName', 'PlayerId', 'Position', 'fivetimeOnIce', 'fiveassists', 'fivegoals','fivepim', 'fiveshots', 'fivehits',
				'fivegames', 'fivepowerPlayGoals', 'fivepowerPlayPoints', 'fivepowerPlayTimeOnIce', 'fiveevenTimeOnIce',
				'fivegameWinningGoals', 'fiveoverTimeGoals', 'fiveshortHandedGoals', 'fiveshortHandedPoints',
				'fiveshortHandedTimeOnIce', 'fiveblocked', 'fiveplusMinus', 'fiveshifts', 'fivepoints']

	df1['tenfanduelpoints']= (df1['tengoals']*12)+(df1['tenassists']*8)+(df1['tenpowerPlayGoals']*0.5)+(df1['tenshortHandedGoals']*2)+(df1['tenshots']*1.60)+(df1['tenblocked']*1.6)
	df2['fivefanduelpoints']= (df2['fivegoals']*12)+(df2['fiveassists']*8)+(df2['fivepowerPlayGoals']*0.5)+(df2['fiveshortHandedGoals']*2)+(df2['fiveshots']*1.60)+(df2['fiveblocked']*1.6)


	df1.to_gbq(
		'hockey_test.ten_game_player_log',
		'dulcet-outlook-227105',
		chunksize=5000,
		reauth=False,
		if_exists='replace',
		credentials = bigquery_key
		)			

	df2.to_gbq(
		'hockey_test.five_game_player_log',
		'dulcet-outlook-227105',
		chunksize=5000,
		reauth=False,
		if_exists='replace',
		credentials = bigquery_key
		)


stats()
gamelogs()

