import matplotlib.pyplot as py
import seaborn as sb
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import google.auth

bigquery_key = service_account.Credentials.from_service_account_file('/Users/Tanner/Desktop/hockey-gbq.json')


# Make clients.
bqclient = bigquery.Client(credentials=bigquery_key, project='dulcet-outlook-227105',)


# Read the CSV File
sql = """
	SELECT
	  *
	FROM
	  `dulcet-outlook-227105.hockey_test.joined`
	"""

job_config = bigquery.QueryJobConfig()
job_config.use_legacy_sql = False

df = bqclient.query(sql, job_config=job_config).result().to_dataframe()
#print(df)

# Sets the X and Y Dataframe Values
X = df[['gameScore', 'xG']]
Y = df[['fanduelpoints']]

# Trains the A.I.
X_train, X_test, y_train, y_test = train_test_split(X, Y)

# Fits the data in the Linear Regression Model
model = LinearRegression()
model.fit(X_train, y_train)

# Tests the Model
predictions = model.predict(X_test)

# Show model statistics
print('Coefficients: ', model.coef_)
print('MSE: ', mean_squared_error(y_test, predictions))
print('R2 Score', r2_score(y_test, predictions))

# Plots the Test and Predictions Chart
# sb.distplot(y_test - predictions, axlabel="Test - Prediction")
# py.show()



# Predicts the Sex given the Input (Change Here)
# 94 -> 94 Kg (Weight)
# 182 -> 182 cm (Height)

# myvals = np.array([94, 182]).reshape(1, -1)
# print(model.predict(myvals))

