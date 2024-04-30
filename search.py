from datetime import datetime, UTC
from sqlalchemy import Column, MetaData, String, Table, DateTime, create_engine
from fastapi import HTTPException
import json
import os
import re


DISCORD_TOKEN = open("DISCORD_TOKEN", "r").read()

class Search:

    def __init__(self, db_url):
        """
        Constructor for the Search class

        :param db_url: The url of the database being searched through
        """
        self.engine = create_engine(db_url)
        metadata = MetaData()

        # Initializes the table within the database 
        id = Column("id", String, primary_key=True, unique=True)
        time = Column("timestamp", DateTime, index = True)
        content = Column("content", String, index = True)
        channel_id = Column("channel_id", String)

        self.messages_tbl = Table("messages", metadata, id, time, channel_id, content)

        # Creates the table if it does not exist 
        metadata.create_all(self.engine)

    def update_database(self, channel_id):
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
        
        conn = self.engine.connect()
        
        # Gets the date of the last exported message 
        query = self.messages_tbl.select().with_only_columns(self.messages_tbl.c.timestamp).\
            order_by(self.messages_tbl.c.timestamp.desc()).limit(1)
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
                   "content": message['content'],
                   "channel_id": channel_id}
            inserts.append(ins)

        if len(inserts) > 0:
            insert_query = self.messages_tbl.insert().values(inserts)
            
            try:
                conn.execute(insert_query)
                conn.commit()
            except:
                raise HTTPException(status_code=500,
                                detail="Failed to insert message into database!")
        conn.close()
        return True
    
    def by_keyword(self, search_term):
        """
        Function to search through exported messages for match with keyword

        Safe from SQL injection attacks as SQLalchemy protects user input from such attacks 
        as long as the input queries aren't raw, string queries

        :param search_term: A string input
        :return: Returns a list JSON that contains the id, timestamp, and content of the message(s) that contains the keyword
        """

        query = self.messages_tbl.select().\
                where(self.messages_tbl.c.content.contains('%' + search_term + '%'))
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchall()

        output = []
        for r in result:
            o = {"id": r[0],
                 "timestamp": r[1],
                 "content": r[3]}
            output.append(o)
        return output

    def by_date(self, start_date, end_date):
        """
        Function to search through exported messages for messages that were sent within the time range

        Made sure user input are in the correct datetime format, raises an exception otherwise
        
        :param start_date: A string start date for the begnning of the search
        :param end_date: A string end date for the end of the search
        :return: Returns a list JSON that contains the ID, timestamp, and content of the message(s) that 
         were sent between start date and end date  
        """
        
        try:
            start = convert_to_datetime(start_date)
            end = convert_to_datetime(end_date)
        except ValueError:
            raise HTTPException(status_code=404,
                                detail="Invalid input. start date and end date does not match format '%Y-%m-%dT%H:%M:%S.%f%z'")

        query = self.messages_tbl.select().\
                where(self.messages_tbl.c.timestamp >= start).\
                where(self.messages_tbl.c.timestamp <= end)
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchall()

        output = []
        for r in result:
            o = {"id": r[0],
                 "timestamp": r[1],
                 "content": r[3]}
            output.append(o)
        return output
        

      

def convert_to_datetime(time):
    """
    Function to convert normal string date time to a datetime object 

    :param time: A string that contains the time input of the user
    :return: Returns the user inputed date as a datetime object
    """

    return datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(UTC)