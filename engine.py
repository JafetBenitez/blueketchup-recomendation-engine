import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://engine:1234567@localhost:5432/blueketchup")

users = pd.read_sql_query("select id from auth_user", con=engine)

query = "select tag_id from core_user_preferences where id = "+str(usr["id"])+";"


def load_user_preferences(self, user_id):
	query = "select tag_id from core_user_preferences where id = "+str(user_id)+";"
	preferences = pd.read_sql_query(query, con=engine)
	return preferences

def load_profile(self, profile_id):
	query = "select tag_id from core_profile_tag where id = "+str(profile_id)+";"
	tags = pd.read_sql_query(query, con=engine)
	return tags

def catalog_preferences(self):

	users = pd.read_sql_query("select id from auth_user", con=engine)
	profiles = pd.read_sql_query("select id from core_profile", con=engine)

	for index, usr in users.iterrows():

		alpha_usr = load_user_preferences(usr["id"])

		for inx, prf in profiles.iterrows():

			beta_profile = load_profile(prf["id"])

			##comparasi[on entre usuario y perfil]
