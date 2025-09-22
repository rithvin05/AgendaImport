#!/usr/bin/env python3
import argparse
import sqlite3
from db_table import db_table

DB_NAME = "interview_test.db"

def fmt(val):
    """Format DB values so None/'None'/'nan' show as empty."""
    return "" if not val or str(val).strip().lower() == "none" else str(val).strip()

def get_agenda_table():
    """Return db_table wrapper for agenda table."""
    return db_table("agenda", {
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

def lookup(column: str, value: str):
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.cursor()
        agenda = get_agenda_table()

        if column == "speaker":
            cur.execute("""
                SELECT a.id, a.title, a.location, a.description, a.type, a.parent_id
                FROM agenda a
                JOIN speakers s ON a.id = s.session_id
                WHERE s.name = ?
            """, (value,))
            rows = cur.fetchall()
        else:
            matches = agenda.select(
                columns=["id", "title", "location", "description", "type", "parent_id"],
                where={column: value}
            )
            rows = [(m["id"], m["title"], m["location"], m["description"], m["type"], m["parent_id"])
                    for m in matches]

        seen, output = set(), []
        for r in rows:
            if r[0] in seen:
                continue
            seen.add(r[0])
            output.append(r)

            if str(r[4]).lower() == "session":
                subs = agenda.select(
                    columns=["id", "title", "location", "description", "type", "parent_id"],
                    where={"parent_id": r[0]}
                )
                for sub in subs:
                    if sub["id"] not in seen:
                        seen.add(sub["id"])
                        output.append((sub["id"], sub["title"], sub["location"],
                                        sub["description"], sub["type"], sub["parent_id"]))

        for _, title, loc, desc, session_type, parent_id in output:
            parent_label = ""
            if str(session_type).lower() == "sub" and parent_id:
                parent = agenda.select(columns=["title"], where={"id": parent_id})
                if parent:
                    parent_label = f"Subsession of {parent[0]['title']}"
            print(f"{fmt(title)}\t{fmt(loc)}\t{fmt(desc)}\t{fmt(session_type)}\t{parent_label}")

        agenda.close()

def main():
    parser = argparse.ArgumentParser(description="Lookup agenda sessions by column and value.")
    parser.add_argument("column", choices=[
        "date", "time_start", "time_end", "title", "location", "description", "speaker"
    ])
    parser.add_argument("value", nargs="+") 
    args = parser.parse_args()

    value = " ".join(args.value) 
    lookup(args.column, value)

if __name__ == "__main__":
    main()
