import time
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable

def connect_to_cassandra(keyspace):
	try:
		auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
		# cluster = Cluster(['172.30.0.11'], port=9042, auth_provider=auth_provider)
		cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)
		session = cluster.connect(keyspace)
		print(f"Successfully connected to the keyspace: {keyspace} ✅")
		return session
	except NoHostAvailable:
		print("Connection failed. No hosts available ❗️")
		return None
	except Exception as e:
		print(f"An error occurred: {e} ❌")
		return None

def fetch_data(session, query):
	rows = session.execute(query)
	column_names = rows.column_names
	data = [row for row in rows]
	df = pd.DataFrame(data, columns=column_names)

	return df

def continuous_fetch_warehouse():
	query = "SELECT * FROM warehouse_data;"
	session = connect_to_cassandra(keyspace='company_one')

	while True:
		data = fetch_data(session, query)
		time.sleep(5)
		yield data

# #check
# df_generator = continuous_fetch_warehouse()
# df = next(df_generator)
# print(df)
