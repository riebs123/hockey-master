from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import traceback
import smtplib
import urllib, json
import pandas as pd
import re
from itertools import permutations
from pulp import *
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

bigquery_key = service_account.Credentials.from_service_account_file('/Users/Tanner/Desktop/hockey-gbq.json')

bqclient = bigquery.Client(credentials=bigquery_key, project='dulcet-outlook-227105',)

job_config = bigquery.QueryJobConfig()
job_config.use_legacy_sql = False

lineup_name = []

def summary(prob):
    div = '---------------------------------------\n'
    print("Variables:\n")
    score = str(prob.objective)
    constraints = [str(const) for const in prob.constraints.values()]
    for v in prob.variables():
        score = score.replace(v.name, str(v.varValue))
        constraints = [const.replace(v.name, str(v.varValue)) for const in constraints]
        if v.varValue != 0:
            print(v.name, "=", v.varValue), lineup_name.append(v.name)
    print(div)
    print("Constraints:")
    for constraint in constraints:
        constraint_pretty = " + ".join(re.findall("[0-9\.]*\*1.0", constraint))
        if constraint_pretty != "":
            print("{} = {}".format(constraint_pretty, eval(constraint_pretty)))
    print(div)
    print("Score:")
    score_pretty = " + ".join(re.findall("[0-9\.]+\*1.0", score))
    print("{} = {}".format(score_pretty, eval(score)))

def optimizer():

	sql = """
	SELECT
	  PlayerName,
	  salary,
	  CASE
		WHEN Position = 'Center' THEN 'C'
		WHEN Position = 'Defenseman' THEN 'D'
		ELSE 'W'
	  END AS Position,
	  ROUND(SUM((gameScore*8.85)+(xG*6.41)),2) AS Value
	FROM
	  `dulcet-outlook-227105.hockey_test.joined`
	GROUP BY
	  PlayerName,
	  Salary,
	  Position
	UNION ALL
	SELECT
	  PlayerName,
	  CAST(Salary as int64) as salary,
	  'G' AS Position,
	  (CAST(Points as NUMERIC)*5)
	FROM
	`dulcet-outlook-227105.hockey_test.goalie_salaries`
	"""

	availables = bqclient.query(sql, job_config=job_config).result().to_dataframe()
	availables = availables.replace('$','') 
	availables = availables.replace(r'        N/A','0', regex=True) 
	availables = availables.replace(r',','', regex=True) 
	availables['salary'] = pd.to_numeric(availables['salary'])
	#print(availables.head())


	salaries = {}
	points = {}
	for pos in availables.Position.unique():
		available_pos = availables[availables.Position == pos]
		salary = list(available_pos[["PlayerName","salary"]].set_index("PlayerName").to_dict().values())[0]
		point = list(available_pos[["PlayerName","Value"]].set_index("PlayerName").to_dict().values())[0]
		salaries[pos] = salary
		points[pos] = point
	#print(salaries["Center"])

	pos_num_available = {
		"C": 3,
		"W": 3,
		"D": 2,
		"G": 1
	}

	salary_CAP = 55000


	_vars = {k: LpVariable.dict(k, v, cat="Binary") for k, v in points.items()}


	prob = LpProblem("Fantasy", LpMaximize)
	rewards = []
	costs = []
	position_constraints = []

	# Setting up the reward
	for k, v in _vars.items():
		costs += lpSum([salaries[k][i] * _vars[k][i] for i in v])
		rewards += lpSum([points[k][i] * _vars[k][i] for i in v])
		prob += lpSum([_vars[k][i] for i in v]) <= pos_num_available[k]
		
	prob += lpSum(rewards)
	prob += lpSum(costs) <= salary_CAP


	prob.solve()

	print(summary(prob))


	#print(lineup_name,lineup_salary,lineup_score)
					
	df1 = pd.DataFrame(lineup_name)
	df1.columns = ['PlayerName']
	df1['Date'] = pd.Timestamp("today").strftime("%m/%d/%Y")

	df1.to_gbq(
		'hockey_test.optimal_lineup',
		'dulcet-outlook-227105',
		chunksize=5000,
		if_exists='append',
		reauth=False,
		credentials = bigquery_key
	)
	
###############EMAIL####################

	full_trace = traceback.format_exc()
	fromaddr = "tannerriebel@hotmail.com"
	toaddr = ['tannerriebel@hotmail.com']

	msg = MIMEMultipart()
	msg['From'] = fromaddr
	msg['To'] = ", ".join(toaddr)
	msg['Subject'] = "Today's Optimal Lineup"
	
	html = """\
	<html>
	  <head></head>
	  <body>
		{0}
	  </body>
	</html>
	""".format(df1.to_html())

	part1 = MIMEText(html, 'html')
	msg.attach(part1)
	server = smtplib.SMTP('smtp.live.com', 587)
	server.starttls()
	server.login(fromaddr, "619991aA")
	text = msg.as_string()
	server.sendmail(fromaddr, toaddr, text)
	server.quit()

optimizer()
