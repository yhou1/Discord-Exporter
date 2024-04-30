from datetime import datetime, UTC
from sqlalchemy import Column, MetaData, String, Table, DateTime, create_engine
import json
import os
import re
from fastapi import HTTPException


DISCORD_TOKEN = open("DISCORD_TOKEN", "r").read()
DB_URL = 'postgresql+psycopg2://postgres:Giftedcheese132@localhost/DiscordMessages'


def get_new_chat(channel_id):
        """
        Function that will export all messages sent within the last 7 days of a specific server to the database
        
        :param channel_id: A string of the channel ID that the user wants the messages pulled from
        """
        # Use the discord chat exporter CLI to fetch messages from the inputed channel ID
        command = "dotnet ./DiscordChatExporter.Cli/DiscordChatExporter.Cli.dll export --channel " + channel_id + " --token " + DISCORD_TOKEN + " --format Json"
        exit_code = os.system(command)
        if exit_code != 0:
            return False 
        
        # Searches through all JSON files with the correct channel ID within the directory using regex
        pattern = r".*\[" + channel_id + r"\]\.json$"  # Match all files ending with ".json"
        filenames = []
        for fn in os.listdir("./"):
            if re.search(pattern, fn):
                filenames.append(fn)
        if len(filenames) != 1:
            return False
        filename  = filenames[0]

        # Opens file and saves messasges to messages variable 
        with open(filename) as p:
            data = json.load(p)
        messages = data['messages']
        
        # Initializes the table within the database 
        engine = create_engine(DB_URL)
        metadata = MetaData()
        conn = engine.connect()

        id = Column("id", String, primary_key=True, unique=True)
        time = Column("timestamp", DateTime, index = True)
        content = Column("content", String, index = True)

        messages_tbl = Table("messages", metadata, id, time, content)

        # Creates the table if it does not exist 
        metadata.create_all(engine)
        
        # Gets the date of the last exported message 
        query = messages_tbl.select().with_only_columns(messages_tbl.c.timestamp).\
            order_by(messages_tbl.c.timestamp.desc()).limit(1)
        last_date = conn.execute(query).fetchall()

        # Goes through messages list. If message timestamp is later than last date, insert into table. Else skip.
        inserts = []
        for message in messages:
            dt_timestamp = datetime.strptime(message['timestamp'], "%Y-%m-%dT%H:%M:%S.%f%z")
            std_timestamp = dt_timestamp.replace(tzinfo = UTC)
            if len(last_date) > 0:
                std_last_date  = last_date[0][0].replace(tzinfo = UTC)

                if std_timestamp <= std_last_date:
                    continue

            ins = {"id": message['id'],
                   "timestamp": dt_timestamp,
                   "content": message['content']}
            inserts.append(ins)

        if len(inserts) > 0:
            insert_query = messages_tbl.insert().values(inserts)
            
            try:
                conn.execute(insert_query)
                conn.commit()
            except:
                raise HTTPException(status_code=500,
                                detail="Failed to insert message into database!")
        conn.close()
        return True