from datetime import datetime, UTC
from sqlalchemy import  MetaData, Table, create_engine
from fastapi import HTTPException

#

DISCORD_TOKEN = open("DISCORD_TOKEN", "r").read()

class Search:

    def __init__(self, db_url):
        """
        Constructor for the Search class

        :param db_url: The url of the database being searched through
        """

        self.engine = create_engine(db_url)
        metadata = MetaData()
        with self.engine.connect() as conn:
            self.messages_tbl = Table("messages",
                                metadata,
                                autoload_with=conn)
    
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
                 "content": r[2]}
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
                 "content": r[2]}
            output.append(o)
        return output
        

      

def convert_to_datetime(time):
    """
    Function to convert normal string date time to a datetime object 

    :param time: A string that contains the time input of the user
    :return: Returns the user inputed date as a datetime object
    """

    return datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(UTC)