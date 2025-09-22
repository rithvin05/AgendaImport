import pandas as pd
import argparse
from db_table import db_table

def main():
    parser = argparse.ArgumentParser(description="A brief description of your script.")
    parser.add_argument("input_file", help="The path to the input file.")

    args = parser.parse_args()
    input_file = args.input_file
    try:
        df = pd.read_excel(input_file, engine='xlrd')

    except FileNotFoundError:
        print(f"The file '{input_file}' was not found.")
    except Exception as e:
        print(f"Error:{e}")

    agenda = db_table("agenda", { "id": "integer PRIMARY KEY", "title": 
                                "text NOT NULL", "start_time": "text NOT NULL", 
                                "end_time": "text NOT NULL","type": "text NOT NULL", "date" : "text NOT NULL",
                                "location": "text", "description": "text", "parent_id": "integer"})
    speakers = db_table("speakers", { "id": "integer PRIMARY KEY", "session_id": "integer NOT NULL", "name": "text NOT NULL",})

    curr_parent_id = None
    start_reading = False
    skip_header = False

    for index, row in df.iterrows():
        first_cell = str(row.iloc[0]).strip()
        if first_cell == "START YOUR AGENDA BELOW":
            start_reading = True
            skip_header = True
            continue
        if not start_reading:
            continue  
        if skip_header:
            skip_header = False
            continue

        title = row['Session Title']
        start_time = row['Time Start']
        end_time = row['Time End']
        date = row['Date']
        type = row['*Session or Sub-session(Sub)']
        location = row['Location'] if pd.notna(row['Location']) else None
        description = row['Description'] if pd.notna(row['Description']) else None
        if type == "Session":
            parent_id = None
        else:
            parent_id = curr_parent_id

        agenda.insert({
            "title": title,
            "start_time": start_time,
            "end_time": end_time,
            "date": date,
            "type": type,
            "location": location,
            "description": description,
            "parent_id": parent_id
        })

        session_id = agenda.db_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        if type == "Session":
            curr_parent_id = session_id

        if pd.notna(row['Speakers']):
            for sp in row['Speakers'].split(';'):
                speakers.insert({
                    "session_id": session_id,
                    "name": sp.strip()
                })


