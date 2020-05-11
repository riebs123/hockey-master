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

bigquery_key = '/Users/User/tanner-project-588591097cf3.json'

client = bigquery.Client.from_service_account_json('/Users/User/tanner-project-588591097cf3.json')

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
	  Salary,
	  CASE
		WHEN Position = 'Center' THEN 'C'
		WHEN Position = 'Defenseman' THEN 'D'
		ELSE 'W'
	  END AS Position,
	  SUM((Playmaker*6.0456)+(HighCeiling*9.9748)+(Consistent*4.6950)+(fanduelpoints*.0797)) AS Value
	FROM
	  `dulcet-outlook-227105.hockey.a_JOINED_FINAL`
	WHERE Salary NOT LIKE '%N/A%'
	GROUP BY
	  PlayerName,
	  Salary,
	  Position
	UNION ALL
	SELECT
	  PlayerName,
	  Salary,
	  'G' AS Position,
	  (CAST(Points as NUMERIC)*5)
	FROM
	`dulcet-outlook-227105.hockey.goalie_salaries`
	WHERE Points NOT LIKE '%N/A%'
	AND SALARY NOT LIKE '%N/A%'
	"""

	availables = client.query(sql, job_config=job_config).result().to_dataframe()
	availables = availables.replace('$','') 
	availables = availables.replace(r'        N/A','0', regex=True) 
	availables = availables.replace(r',','', regex=True) 
	availables['Salary'] = pd.to_numeric(availables['Salary'])
	#print(availables.head())


	salaries = {}
	points = {}
	for pos in availables.Position.unique():
		available_pos = availables[availables.Position == pos]
		salary = list(available_pos[["PlayerName","Salary"]].set_index("PlayerName").to_dict().values())[0]
		point = list(available_pos[["PlayerName","Value"]].set_index("PlayerName").to_dict().values())[0]
		salaries[pos] = salary
		points[pos] = point
	#print(salaries["Center"])

	pos_num_available = {
		"C": 2,
		"W": 4,
		"D": 2,
		"G": 1
	}

	SALARY_CAP = 55000


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
	prob += lpSum(costs) <= SALARY_CAP


	prob.solve()

	print(summary(prob))


	#print(lineup_name,lineup_salary,lineup_score)
					
	df1 = pd.DataFrame(lineup_name)
	df1.columns = ['PlayerName']
	
###############EMAIL####################

	full_trace = traceback.format_exc()
	fromaddr = "tannerriebel@hotmail.com"
	toaddr = ['hurle157@d.umn.edu', 'tannerriebel@hotmail.com', 'judreinke@gmail.com','mitch@aimclear.com']

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
