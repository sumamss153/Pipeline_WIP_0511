import pandas as pd #pd is the common alias of pandas library
import os
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from db_connection import connect_to_db
from config import DATA_FOLDER
from sqlalchemy import create_engine

# Extract step: Read the CSV file
def extract_data():

    df = pd.read_csv(r"C:\Users\Sumam.Selvin\Downloads\dqthon-participants.csv")
    print(df.head(5))
    print(df.info())
    print(f"Data extracted successfully.")
    return df

def transform_data(df):

    df["postal_code"] = df["address"].str.extract(r"(\d+)$")
    print(df["postal_code"].head())

    df['city'] = df['address'].str.extract(r'(?<=\n)(\w.+)(?=,)')
    print(df['city'].head())

    df['github_profile'] = 'https://github.com/' + df['first_name'].str.lower() + df['last_name'].str.lower()
    print(df['github_profile'].head())

    df['cleaned_phone_number'] = df['phone_number'].str.replace(r'^(\+62|62)', '0')
    df['cleaned_phone_number'] = df['cleaned_phone_number'].str.replace(r'[()-]', '')
    df['cleaned_phone_number'] = df['cleaned_phone_number'].str.replace(r'\s+', '')
    print(df['cleaned_phone_number'].head())

    def team(col):
        abbrev_name = "%s%s"%(col['first_name'][0],col['last_name'][0])
        country = col['country']
        abbrev_institute = '%s'%(''.join(list(map(lambda word: word[0], col['institute'].split()))))
        return "%s-%s-%s"%(abbrev_name,country,abbrev_institute)

    df['team_name'] = df.apply(team, axis=1)
    print(df['team_name'].head())

    def func(col):
        first_name_lower = col['first_name'].lower()
        last_name_lower = col['last_name'].lower()
        institute = ''.join(list(map(lambda word: word[0], col['institute'].lower().split()))) #Singkatan dari nama perusahaan dalam lowercase

        if 'Universitas' in col['institute']:
            if len(col['country'].split()) > 1: #Kondisi untuk mengecek apakah jumlah kata dari country lebih dari 1
                country = ''.join(list(map(lambda word: word[0], col['country'].lower().split())))
            else:
                country = col['country'][:3].lower()
            return "%s%s@%s.ac.%s"%(first_name_lower,last_name_lower,institute,country)

        return "%s%s@%s.com"%(first_name_lower,last_name_lower,institute)

    df['email'] = df.apply(func, axis=1)
    print(df['email'].head())

    df['birth_date'] = pd.to_datetime(df['birth_date'], format='%d %b %Y')
    print(df['birth_date'].head())

    df['register_at'] = pd.to_datetime(df['register_time'], unit='s')
    print(df['register_at'].head())
    print(df)

def load_data_to_sql(df, conn_string):
    try:
        df.to_sql('participants', con=conn_string, if_exists='append', index=False)
        # Print the IDs of activities added
        added_ids = df['participant_id'].tolist()
        print(f"Activities with the following IDs have been added: {added_ids}")
    except SQLAlchemyError as e:
        print(f"Error occurred while inserting data: {e}")

# Main ETL function
def etl_pipeline():
    engine = connect_to_db()
    # Extract
    df = extract_data()

    # Transform
    df_transformed = transform_data(df)

    # Load
    load_data_to_sql(df, engine)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    etl_pipeline()
