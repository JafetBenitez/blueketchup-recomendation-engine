import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://engine:1234567@localhost:5432/blueketchup")

tags = pd.read_sql_query("select id from core_tags", con=engine)

for index, tg in tags.iterrows():
    
