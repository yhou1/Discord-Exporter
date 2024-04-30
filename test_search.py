import unittest
from search import Search
from datetime import datetime, timedelta, UTC
from fastapi import HTTPException


class TestStringMethods(unittest.TestCase):
    db_url = 'postgresql+psycopg2://postgres:Giftedcheese132@localhost/DiscordMessages'
    channel_id = '1233269423113109524'
    
    search_db = Search(db_url)

    def test_search_by_keyword(self):
        result = self.search_db.by_keyword("Test")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["content"],"Test 1")
        self.assertEqual(result[1]["content"], "Test 2")
        
        result = self.search_db.by_keyword("Test 1")
        self.assertEqual(len(result),  1)
        self.assertEqual(result[0]["content"], "Test 1")
        
        result = self.search_db.by_keyword("Test 2")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["content"], "Test 2")

        result = self.search_db.by_keyword("does not exist")
        self.assertEqual(len(result), 0)
        
        result = self.search_db.by_keyword("")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["content"], "Test 1")
        self.assertEqual(result[1]["content"], "Test 2")

        result = self.search_db.by_keyword(" ")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["content"], "Test 1")
        self.assertEqual(result[1]["content"], "Test 2")

    def test_search_by_date(self):
        # test search outputs multiple messages
        start_date = "2024-04-24T21:12:28.817-07:00"
        end_date = "2024-04-26T21:12:30.351-07:00"
        result = self.search_db.by_date(start_date, end_date)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["content"], "Test 1")
        self.assertEqual(result[1]["content"], "Test 2")

        # test search excludes 2nd message
        start_date = "2024-04-25T21:12:28.817-07:00"
        end_date = "2024-04-25T21:12:29.351-07:00"
        result = self.search_db.by_date(start_date, end_date)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["content"], "Test 1")

        # test search excludes 1st message
        start_date = "2024-04-25T21:12:29.351-07:00"
        end_date = "2024-04-25T21:12:31.351-07:00"
        result = self.search_db.by_date(start_date, end_date)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["content"], "Test 2")
        
        # test search in past should get no messages
        start_date = "2020-04-25T21:12:29.351-07:00"
        end_date = "2020-04-25T21:12:31.351-07:00"
        result = self.search_db.by_date(start_date, end_date)
        self.assertEqual(len(result), 0)

        # test search in future should get no messages
        start_date = datetime.now().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        end_date = (datetime.now() + timedelta(days=1)).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        result = self.search_db.by_date(start_date, end_date)
        self.assertEqual(len(result), 0)
    
    def test_search_by_date_invalid_input(self):
        # test date is not valid date
        start_date = "2020-158-25T21:12:31.351-07:00"
        end_date = "2020-13-25T21:12:31.351-07:00"
        self.assertRaises(HTTPException, self.search_db.by_date, start_date, end_date)

        # test not a date
        start_date = "invalid start"
        end_date = "2020-04-25T21:12:31.351-07:00"
        self.assertRaises(HTTPException, self.search_db.by_date, start_date, end_date)

        # test date does not follow expected format
        start_date = "2020-04-25"
        end_date = "2020-04-26"
        self.assertRaises(HTTPException, self.search_db.by_date, start_date, end_date)
        

if __name__ == '__main__':
    unittest.main()