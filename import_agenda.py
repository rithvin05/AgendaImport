#!/usr/bin/env python3
import pandas as pd
import argparse
from db_table import db_table

def clean(val):
    """Convert to string, strip whitespace, handle None/nan safely."""
    if pd.isna(val):
        return None
    return str(val).strip().replace("'", "''") 

def load_dataframe(path: str) -> pd.DataFrame:
    """Load agenda Excel, starting after the 'START YOUR AGENDA BELOW' marker."""
    df_raw = pd.read_excel(path, engine="xlrd", header=None)
    start_row = df_raw[df_raw.iloc[:, 0].astype(str).str.contains(
        "START YOUR AGENDA BELOW", na=False
    )].index[0]
    df = pd.read_excel(path, engine="xlrd", header=start_row + 1)

    column_map = {
        '*Date': 'Date',
        '*Time Start': 'Time Start',
        '*Time End': 'Time End',
        '*Session or \nSub-session(Sub)': 'Type',
        '*Session Title': 'Session Title',
        'Room/Location': 'Location',
        'Description': 'Description',
        'Speakers': 'Speakers'
    }
    return df.rename(columns=column_map)

def main():
    parser = argparse.ArgumentParser(description="Import agenda Excel into SQLite DB.")
    parser.add_argument("input_file", help="Path to the input Excel file")
    args = parser.parse_args()

    try:
        df = load_dataframe(args.input_file)
    except Exception as e:
        print(f"Error reading Excel: {e}")
        return

    agenda = db_table("agenda", {
        "id": "integer PRIMARY KEY",
        "title": "text NOT NULL",
        "time_start": "text NOT NULL",
        "time_end": "text NOT NULL",
        "type": "text NOT NULL",
        "date": "text NOT NULL",
        "location": "text",
        "description": "text",
        "parent_id": "integer"
    })

    speakers = db_table("speakers", {
        "id": "integer PRIMARY KEY",
        "session_id": "integer NOT NULL",
        "name": "text NOT NULL"
    })

    curr_parent_id = None

    for _, row in df.iterrows():
        values = {col: clean(row[col]) for col in [
            "Session Title", "Time Start", "Time End", "Date", "Type", "Location", "Description"
        ]}

        parent_id = None if values["Type"] == "Session" else curr_parent_id

        agenda.insert({
            "title": values["Session Title"],
            "time_start": values["Time Start"],
            "time_end": values["Time End"],
            "date": values["Date"],
            "type": values["Type"],
            "location": values["Location"],
            "description": values["Description"],
            "parent_id": parent_id
        })

        session_id = agenda.db_conn.execute(
            "SELECT last_insert_rowid()"
        ).fetchone()[0]

        if values["Type"] == "Session":
            curr_parent_id = session_id

        if pd.notna(row["Speakers"]):
            for sp in str(row["Speakers"]).split(';'):
                speakers.insert({
                    "session_id": session_id,
                    "name": clean(sp)
                })

    agenda.close()
    speakers.close()

if __name__ == "__main__":
    main()
