
import pymongo
from pymongo import MongoClient
import os
import pandas as pd
import certifi
from pathlib import Path
import json
from bson import json_util, ObjectId
from pandas.io.json import json_normalize

cluster = MongoClient("mongodb+srv://assignment:wlF8axz8N4ZvqH6B@assignment.h251h.mongodb.net/", tlsCAFile=certifi.where())

print("Connection successful")

db = cluster["sample_mflix"]
col_com = db["comments"]
col_mov = db["movies"]
col_theatre = db["theaters"]


def conn_mongo_map():

    comments = col_com.find_one()
    movies = col_mov.find_one() 
    
    path = "D:\Mongo_db_task\Movies"
    if not os.path.exists(path):
        os.makedirs(path)

    #Path(path).mkdir(parents=True, exist_ok=True)
    
    function1_lookup_comments = {
       "$lookup": {
             "from": "comments", 
             "localField": "_id", 
             "foreignField": "movie_id", 
             "as": "mv_comments",
       }
    }
    

    pipeline = [
        function1_lookup_comments,
    ]
   
    results = col_mov.aggregate(pipeline)
   
    for movie in results:
        #print(movie)
        datafr_1 = pd.DataFrame([movie])
        #datafr_1.to_csv('D:\Mongo_db_task\Movies\movies_with_comments.csv')
        
        low_runtime = []
        high_runtime = []
        for i in datafr_1['runtime']:
            if i > 50:
                low_runtime.append('yes')
                high_runtime.append('no')
            else:
                low_runtime.append('no')
                high_runtime.append('yes')
        datafr_1['low_runtime'] = low_runtime
        datafr_1['high_runtime'] = high_runtime
        datafr_1.to_csv('D:\Mongo_db_task\Movies\movies_with_comments.csv')
        
        break
       
       
def get_rating_released_after2k():
       
#    match_imdb_rating = {
#        "$match": {
#            "imdb": {
#                "rating": {
#                    "$gt": 8,
#                }
#            }
#        }
#    }
    
   
#    match_year_on_or_before_2k = {
#        "$match": {
#            "year": {
#                "$ge": 2000,
#            }
#        }
#    }
    match_awards = {
        "$match": {
            "awards": {
                "$gt": 3,
            }
        }
    }
    stage_sort_released_descending = {
        "$sort": { 
            "released": pymongo.DESCENDING
            }
    }
    
    
    
    pipeline = [
#        match_imdb_rating,
#        match_year_on_or_before_2k,
        match_awards,
        stage_sort_released_descending,
    ]
   
    results = col_mov.aggregate(pipeline)
    
    for movie_ratings in results:
        #print(movie_ratings)
        datafr_1 = pd.DataFrame([movie_ratings])
        datafr_1.to_csv('D:\Mongo_db_task\Movies\movies_rating_8_released_aft_2000.csv ')

def movies_outside_usa():

    match_movies_outside_usa = {
        "$match": {
            "countries": {
                "$ne": 'USA',
            }
        }
    }
    
    pipeline = [
        match_movies_outside_usa,
    ]

    results = col_mov.aggregate(pipeline)
     
    for outside_india in results:
        data_frame_1 = pd.DataFrame([outside_india])
        data_frame_1.to_csv('D:\Mongo_db_task\Movies\movies_released_outside_usa.csv ')

def theatre_col_rearrange():
    
    #print(col_theatre.find_one())
    theatre_data = col_theatre.find_one()
    theatre_data_load = json.loads(json_util.dumps(theatre_data))
    normalize_data = pd.json_normalize(theatre_data_load['location'])
    df = pd.DataFrame(normalize_data)
    
    #For rearrange
    df.to_csv('D:/Mongo_db_task/Movies/theatre_simplified.csv  ')
    
    df_1 = df.reindex(columns=['street1' , 'city', 'street2', '0' , '1'])
    
    df_1.to_csv('D:/Mongo_db_task/Movies/theatre_simplified_1.csv  ')
    
    

conn_mongo_map()
#get_rating_released_after2k()
movies_outside_usa()
theatre_col_rearrange()


