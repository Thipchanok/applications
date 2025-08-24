import pandas as pd
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost/job_application_db')

df = pd.read_csv(r'D:\subdistricts.csv')
df.columns = df.columns.str.strip()
df['subdistrict_name'] = df['subdistrict_name'].str.strip()
df['district_id'] = df['district_id'].astype(int)
df['subdistrict_id'] = df['subdistrict_id'].astype(int)

df = df.drop_duplicates(subset=['subdistrict_id'])

existing_districts = pd.read_sql('SELECT district_id FROM districts', engine)
existing_districts_set = set(existing_districts['district_id'])
df = df[df['district_id'].isin(existing_districts_set)]

existing_subs = pd.read_sql('SELECT subdistrict_id FROM subdistricts', engine)
existing_subs_set = set(existing_subs['subdistrict_id'])
df = df[~df['subdistrict_id'].isin(existing_subs_set)]

print(f"Rows ที่จะ import: {len(df)}")

try:
    df.to_sql('subdistricts', con=engine, if_exists='append', index=False)
    print("Import subdistricts succeed")
except Exception as e:
    print("error import:", e)
