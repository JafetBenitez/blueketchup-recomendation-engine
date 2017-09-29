import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics import jaccard_similarity_score
import logging

logging.basicConfig(
	filename='blueketchup-ml.log',
	level=logging.DEBUG,
	format='%(asctime)s |%(levelname)s : %(message)s',
	datefmt='%m/%d/%Y %I:%M:%S')
CONNECTION_STRING = "postgresql://engine:1234567@localhost:5432/blueketchup"



def load_user_preferences(user_id):
	logging.debug("load_user_preferences({0})".format(user_id))
	engine = create_engine(CONNECTION_STRING)

	query = """
			SELECT
				id,
				COALESCE(
					(
					SELECT 1
					FROM core_user_preferences
					WHERE tag_id = core_tag.id AND user_id = {0}),
					0) AS like
			FROM core_tag;
			""".format(user_id)

	preferences = pd.read_sql_query(query, con=engine)

	return preferences

def load_profile(profile_id):
	logging.debug("load_profile()")
	engine = create_engine(CONNECTION_STRING)
	query = """
			SELECT
				id,
				COALESCE(
					(
					SELECT 1
					FROM core_profile_tags
					WHERE tag_id = core_tag.id AND profile_id = {0}),
					0) as like
			FROM core_tag;
			""".format(profile_id)

	profile = pd.read_sql_query(query, con=engine)

	return profile

def catalog_preferences():
	logging.debug("catalog_preferences()")
	engine = create_engine(CONNECTION_STRING)
	users = pd.read_sql_query("select id from auth_user", con=engine)
	profiles = pd.read_sql_query("select id from core_profile", con=engine)

	for index, usr in users.iterrows():

		user_preferences = load_user_preferences(usr["id"])
		is_cataloged = False
		for inx, prf in profiles.iterrows():
			profile_tags = load_profile(prf["id"])
			similarity = jaccard_similarity_score(user_preferences["like"]
				.tolist(), profile_tags["like"].tolist())

			logging.info('Similitud entre el usuario {0} y el perfil {1} : {2}'
				.format(usr["id"], prf["id"], similarity))
			connection = engine.connect()
			if similarity > 0.5:
				try:
					is_cataloged = True
					connection.execute(
					"""
						INSERT INTO core_profile_users (user_id, profile_id)
						VALUES ({0}, {1}) RETURNING id;
					""".format(usr["id"], prf["id"]))
					logging.info('Se ha incluido el usuario {0} en el perfil {1}'
						.format(usr["id"], prf["id"]))

				except Exception as e:
					logging.error(e)
		if not is_cataloged:
			try:
				new_profile_name = "Alpha-"+str(usr["id"])
				new_profile = connection.execute(
				"""
					INSERT INTO core_profile (name)
					VALUES ('{0}') RETURNING id;
				""".format(new_profile_name))

				print(new_profile)

				logging.info('Se ha creado un nuevo perfil : {1}'
					.format(new_profile_name))

				connection.execute(
				"""
					INSERT INTO core_user_profiles (user_id, profile_id)
					VALUES ({0}) RETURNING id;
				""".format(usr["id"], new_profile["id"]))





			except Exception as e:
				logging.error(e)

			logging.info("That´s all")


catalog_preferences()
