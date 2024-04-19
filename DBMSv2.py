# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 22:20:42 2024

@author: soso2
"""

import sqlite3
import pandas as pd
import os


def get_temp_dir(data_dir="my_data"):
  """
  Gets the temporary storage directory path.

  Args:
      data_dir (str, optional): The desired directory name within the temporary storage. Defaults to "my_data".

  Returns:
      str: The full path to the temporary directory.
  """
  temp_dir = os.path.join(os.getenv("RUNNER_TEMP"), data_dir)
  os.makedirs(temp_dir, exist_ok=True)
  return temp_dir


def connect_to_db(db_name="users.db"):
  """
  Connects to the SQLite database file.

  Args:
      db_name (str, optional): The name of the database file. Defaults to "users.db".

  Returns:
      sqlite3.Connection: The connection object to the database.
  """
  db_path = os.path.join(get_temp_dir(), db_name)
  conn = sqlite3.connect(db_path)
  return conn

def create_tables(conn):
  """
  Creates tables in the database if they don't exist.

  Args:
      conn (sqlite3.Connection): The connection object to the database.
  """
  c = conn.cursor()
  # Define your table schema here (e.g., user_id INTEGER PRIMARY KEY, username TEXT)
  table_creation_query ='''CREATE TABLE IF NOT EXISTS users (
        ID INTEGER PRIMARY KEY,
        Username TEXT,
        firstname TEXT NOT NULL,
        lastname TEXT
)'''
  c.execute(table_creation_query)
  conn.commit()


def insert_users(conn,ID,username,first,last):
  """
  Inserts ID,username,first,last

  Args:
      conn (sqlite3.Connection): The connection object to the database.
      data (list): A list of dictionaries containing user information (e.g., {"user_id": 1, "username": "Alice"}).
  """
  c = conn.cursor()
  try:
      c.execute("INSERT INTO users(ID, Username, firstname, lastname) VALUES (?, ?, ?, ?)", (ID, username, first, last))
      conn.commit()
  except sqlite3.IntegrityError:
      # If the ID already exists, update the user's first and last name
      c.execute("UPDATE users SET firstname=?, lastname=?, Username=? WHERE ID=? ", (first, last , username,ID))
      conn.commit()


def fetch_data(conn):
  """
  Fetches data from the database using a query.

  Args:
      conn (sqlite3.Connection): The connection object to the database.

  Returns:
      pandas.DataFrame: The retrieved data as a DataFrame.
  """
  c = conn.cursor()
  query = "SELECT * FROM users"  # Replace with your specific query if needed
  df = pd.read_sql_query(query, conn)
  return df


def export_to_csv(df, csv_name="users.csv", encoding="utf-16", sep="\t"):
  """
  Exports the data from a DataFrame to a CSV file.

  Args:
      df (pandas.DataFrame): The DataFrame containing the data to export.
      csv_name (str, optional): The desired name for the CSV file. Defaults to "users.csv".
      encoding (str, optional): The character encoding for the CSV file. Defaults to "utf-16".
      sep (str, optional): The field separator to use in the CSV file. Defaults to "\t" (tab).
  """
  csv_path = os.path.join(get_temp_dir(), csv_name)
  df.to_csv(csv_path, index=False, encoding=encoding, sep=sep)
  print(f"Database converted to CSV: {csv_path}")


