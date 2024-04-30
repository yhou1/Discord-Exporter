from fastapi import FastAPI
from search import Search
from new_chat import update_database

app = FastAPI()

db_url = 'postgresql+psycopg2://postgres:Giftedcheese132@localhost/DiscordMessages'

search = Search(db_url)

@app.get("/")
def root():
    return {"message": "Hello World"}

@app.get("/term_search")
def term_search(keyword):
    chats = search.by_keyword(keyword)
    return chats

@app.get("/date_search")
def time_search(start_date, end_date):
    chats = search.by_date(start_date, end_date)
    return chats

@app.post("/update_chat")
def get_new_chat(channel_id):
    isSuccess = update_database(channel_id)
    if not isSuccess:
        return "Failed"
    return "Success!"