import pymongo
import os
import pandas as pd
from pymongo import MongoClient


client = MongoClient()

    #Change the connection URL which they hv provided
client = MongoClient('Mongo URI')
    #give the database name , they hv provided
db = client['sample_mflix']
#select the collection within the database
test = db.test
#convert entire collection to Pandas dataframe
test = pd.DataFrame(list(test.find()))
    

#NOTE: https://www.mongodb.com/docs/atlas/sample-data/sample-mflix/#std-label-mflix-comments 
              
def _map_collections():
    
    #Function-1
            # 1. In the below link, we hv two collections(sample_mflix.comments,sample_mflix.movies)
                    # https://www.mongodb.com/docs/atlas/sample-data/sample-mflix/#std-label-mflix-comments
            # 2. You have to map these two collection(means Join) and also identify the primary and foreign key.
                    # reference: https://www.kaggle.com/code/deepak525/investigate-tmdb-movie-dataset
            # 3. create movies folder using python only. Include below lines in python function-1 
                        #import os
                        # os.mkdir('movies')
            # 3. After mapping create two dataframes using pandas one with movies_with_comments and movies_without_comments.csv files under movies dir.
                    #reference: https://www.javatpoint.com/how-to-create-a-dataframes-in-python
                    
    #<Source code here>                
def _add_columns_to_dataframe():
            #Function-2
            # 1. Add two more cloumns to the "movies_with_comments" dataframe called "low_runtime" and "high_runtime"
            # 2. Write a query based on the condition provided in the second function, fill the values of newly added columns
            # 3. Save as Movies_with_comments.csv file into MOvies folder again.
            
    #Function-2 source code here <>


def _get_movie_collection_data():
            #Function-3
            # 1. Query the "movies" collection , get all the movies having more than 8 rating and released on or after 2000 with more than 3 awards in ascending order of released.
            
                            #EXAMPLE:
                                #collection = db.collection_name
                                #data = pd.DataFrame(list(collection.find(<your query based on above condition here>)))
                                
            # 2. Save this result into movies_rating_8_released_aftr_2000.csv file
            
    #Function-3 code goes here
    
def _theatre_collection_data():
    
        #Same as above you can query the theatre collection
  