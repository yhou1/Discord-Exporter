from fastapi import FastAPI
from search import Search

app = FastAPI()

db_url = 'postgresql+psycopg2://postgres:Giftedcheese132@localhost/DiscordMessages'

search = Search(db_url)

@app.get("/")
def root():
    return {"message": "Hello World"}

@app.post("/update_chat")
def get_new_chat(channel_id):
    """
    get_new_chat takes in a channel ID, inserting all messages sent in the channel 
    within the last 7 days into the database
    """
    isSuccess = search.update_database(channel_id)
    if not isSuccess:
        return "Failed"
    return "Success!"

@app.get("/term_search")
def term_search(keyword):
    """
    term_search takes in keyword input, returning all messages that contains the keyword
    
    :return: Return a list of JSON
    """
    chats = search.by_keyword(keyword)
    return chats

@app.get("/date_search")
def time_search(start_date, end_date):
    """
    time_search takes in a start and end date, returning all messages sent within that time range
    
    :return: Return a list of JSON"""
    chats = search.by_date(start_date, end_date)
    return chats
