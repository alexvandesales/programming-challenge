import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from sqlite3 import Error
from flask import Flask, jsonify
from flask_cors import CORS, cross_origin
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn
def insert_data_titleratings(database):
    try:
        file = "title_ratings.tsv"
        db = "sqlite:///" + database
        tsv_database = create_engine(db)
        chunksize = 10000
        i = 0
        j = 0
        for df in pd.read_csv(file, chunksize=chunksize, iterator=True, delimiter="\t"):
            df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
            df.index += j
            df.to_sql('titleratingsdata', tsv_database, if_exists='append')
            j = df.index[-1] + 1
            df = pd.read_sql_query('select * from titleratingsdata', tsv_database)
        return df
    except Error as e:
        print(e)

def insert_data_namebasics(database):
    try:
        file = "name_basics.tsv"
        db = "sqlite:///" + database
        tsv_database = create_engine(db)
        chunksize = 10000
        i = 0
        j = 0
        for df in pd.read_csv(file, chunksize=chunksize, iterator=True, delimiter="\t"):
            df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
            df.index += j
            df.to_sql('namebasicsdata', tsv_database, if_exists='append')
            j = df.index[-1] + 1
            df = pd.read_sql_query('select * from namebasicsdata', tsv_database)
        return df
    except Error as e:
        print(e)

def insert_data_titlebasics(database):
    try:
        file = "title_basics.tsv"
        db = "sqlite:///" + database
        tsv_database = create_engine(db)
        chunksize = 10000
        i = 0
        j = 0
        for df in pd.read_csv(file, chunksize=chunksize, iterator=True, delimiter="\t"):
            df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
            df.index += j
            df.to_sql('titlebasicsdata', tsv_database, if_exists='append')
            j = df.index[-1] + 1
            df = pd.read_sql_query('select * from titlebasicsdata', tsv_database)
            #select que retorna a consulta de filmes ordenados pela media
            #"""select b.originalTitle, averageRating, numVotes from title_ratings as a
            #   inner join title_basics as b on a.tconst = b.tconst
            #   order by a.averageRating DESC"""
        return df
    except Error as e:
        print(e)

def create_table(conn, sql_create_title_ratings_table, sql_create_title_basics_table, sql_create_name_basics_table):
    try:
        c = conn.cursor()
        c.execute(sql_create_title_ratings_table)
        c.execute(sql_create_title_basics_table)
        c.execute(sql_create_name_basics_table)

    except Error as e:
        print(e)

@app.route('/')
def main():
    try:
        database = r"testePython.db"

        sql_create_title_ratings_table = """ CREATE TABLE IF NOT EXISTS title_ratings (
                                                tconst text PRIMARY KEY,
                                                averageRating real,
                                                numVotes integer
                                            ); """

        sql_create_title_basics_table = """ CREATE TABLE IF NOT EXISTS title_basics (
                                                    tconst text PRIMARY KEY,
                                                    titleType text,
                                                    primaryTitle text,
                                                    originalTitle text,
                                                    isAdult integer, 
                                                    startYear text,
                                                    endYear text,
                                                    runtimeMinutes integer,
                                                    genres text
                                                ); """

        sql_create_name_basics_table = """ CREATE TABLE IF NOT EXISTS name_basics (
                                                    tconst text PRIMARY KEY,
                                                    primaryName text,
                                                    birthYear text,
                                                    deathYear text,
                                                    primaryProfession text,
                                                    knownForTitles text
                                                ); """

        conn = create_connection(database)
        if conn is not None:
            create_table(conn, sql_create_title_ratings_table,
                         sql_create_title_basics_table,
                         sql_create_name_basics_table)
            df_titleratings = insert_data_titleratings(database)
            df_titlebasics = insert_data_titlebasics(database)
            df_namebasics = insert_data_namebasics(database)

            out_titleratings = df_titleratings.to_json(orient='records')
            out_titlebasics = df_titlebasics.to_json(orient='records')
            out_namebasics = df_namebasics.to_json(orient='records')

            return out_df_titlebasics

        else:
            print("Error! cannot create the database connection.")
    except Error as e:
        print(e)

if __name__ == '__main__':
    main()
