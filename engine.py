import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics import jaccard_similarity_score

CONNECTION_STRING = "postgresql://engine:1234567@localhost:5432/blueketchup"



#users = pd.read_sql_query("select id from auth_user", con=engine)

#query = "select tag_id from core_user_preferences where id = "+str(usr["id"])+";"


def load_user_preferences(user_id):
	engine = create_engine(CONNECTION_STRING)
	#query = "select tag_id from core_user_preferences where id = "+str(user_id)+";"

	query = """
			select core_tag.id as tag, case when core_user_preferences is null then 0 else 1 end as value
			from auth_user as users
			left join core_user_preferences on core_user_preferences.user_id = users.id
			left join core_tag on  core_user_preferences.tag_id = core_tag.id
			where users.id = {0}
			""".format(user_id)

	preferences = pd.read_sql_query(query, con=engine)
	return preferences

def load_profile(profile_id):
	engine = create_engine(CONNECTION_STRING)
	#query = "select tag_id from core_profile_tag where id = {profile_id};"

	query = """
			select core_tag.id as tag, case when core_profile_tags is null then 0 else 1 end as value
			from core_profile as profile
			left join core_profile_tags on core_profile_tags.profile_id = profile.id
			left join core_tag on  core_profile_tags.tag_id = core_tag.id
			where profile.id = {0}
			""".format(profile_id)
	profile = pd.read_sql_query(query, con=engine)
	return profile

def catalog_preferences():
	engine = create_engine(CONNECTION_STRING)
	users = pd.read_sql_query("select id from auth_user", con=engine)
	profiles = pd.read_sql_query("select id from core_profile", con=engine)

	for index, usr in users.iterrows():

		alpha_usr = load_user_preferences(usr["id"])
		print("user {0}".format(usr["id"]))

		for inx, prf in profiles.iterrows():
			print("profile {0}".format(prf["id"]))

			beta_profile = load_profile(prf["id"])

			print(beta_profile["value"])
			print(jaccard_similarity_score(alpha_usr["value"], beta_profile["value"]))
			print("That´s all")


catalog_preferences()
