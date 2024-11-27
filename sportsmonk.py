import requests
import pandas as pd
import numpy as np
import config as cfg
import os
import openpyxl

################################################
# Get sportsmonk data for week range, week_start to week_end 
################################################
def Get_Sportmonks_Data(week_start:int, week_end:int):    
    # populate list_all_data with fixture data 
    list_all_data = [] # init 
    Call_Sportmonks_Api(cfg.api_url,list_all_data)
    #print (list_all_data)

    # Get The Top Level Fixture Data from the list and put it into a data frame 
    df_top_level = Get_Top_Level_Data(list_all_data,week_start,week_end)
    df_top_level.to_csv(os.path.join("csv", "top_level.csv"), index=False)

    # Get the score data from the list and put it into a data frame 
    df_scores = Get_Score_Data(list_all_data)
    df_scores.to_csv(os.path.join("csv", "scores.csv"), index=False)

    # Merge the top-level data with the scores data on 'id' and 'scores.fixture_id'
    df_all_flatten = df_top_level.merge(df_scores, left_on='id', right_on='scores.fixture_id', how='left') 
    df_all_flatten.to_csv(os.path.join("csv", "df_all_flatten.csv"), index=False)

    # write all_data to excel
    # df_top_level.to_excel("df_flatten.xlsx")
    # df_top_level.to_csv("csv/top_level.csv", index=False)
    
################################################
# Call SportsMonk API
################################################

def Call_Sportmonks_Api(url, list_all_data):    

    print(url)
    # Fetch data from API (first page)
    jsn_response = requests.get(url).json()
    # Check if data is empty
    if not jsn_response.get("data"):

        print("No data available in response.")

        return
        
    # Add the first page of data to all_data
    list_all_data.extend(jsn_response["data"])

####################

# Get top level Data

####################

def Get_Top_Level_Data(list_all_data,start_week,end_week):

    # GET THE TOP LEVEL FIXTURE DATA

    df_top_level = pd.DataFrame(list_all_data, columns=['id', 'round_id', 'name'])

    # Sort df_top_level by round_id to ensure ordering from min to max

    df_top_level = df_top_level.sort_values(by='round_id').reset_index(drop=True)

    # factorize() to map unique round_ids starting from 1

    df_top_level['week'] = pd.factorize(df_top_level['round_id'])[0] + 1    

    # Filter df_top_level where week is between start_week and end_week (inclusive)

    df_top_level = df_top_level[df_top_level['week'].between(start_week, end_week)]

    return df_top_level

####################

# Get The Score Data

####################

def Get_Score_Data(list_all_data):

    # GET THE FIXTURE SCORES DATA

    # Normalize scores record

    df_scores = pd.json_normalize(list_all_data, record_path=["scores"], record_prefix="scores.")

    # Filter the scores data to only include the score after the end of the second half

    # debug - use for score fixtire testing : df_scores = df_scores[(df_scores['scores.type_id'] == 2) & (df_scores['scores.fixture_id'].isin([18861937, 18861938]))]
    df_scores = df_scores[(df_scores['scores.type_id'] == 2)] 

    # Drop unwanted column data from scores
    df_scores = df_scores[['scores.fixture_id', 'scores.description', 'scores.score.goals', 'scores.score.participant']]
 
    # Create folder if it doesn't exist
    #os.makedirs(folder, exist_ok=True)

    # Get the scores on a single row and reorder columns to ensure home_goals comes before away_goals
    df_scores = df_scores.pivot_table(index='scores.fixture_id', columns='scores.score.participant', values='scores.score.goals') \
        .rename(columns={'home': 'home_goals', 'away': 'away_goals'})[['home_goals', 'away_goals']].reset_index()
 
    # Convert home_goals and away_goals to integers
    df_scores[['home_goals', 'away_goals']] = df_scores[['home_goals', 'away_goals']].fillna(0).astype(int)

    # Add a new 'result' column to df_scores 
    df_scores['result'] = np.where(df_scores['home_goals'] > df_scores['away_goals'], 'Home',
                          np.where(df_scores['away_goals'] > df_scores['home_goals'], 'Away', 'Draw'))

    # Add a new 'score.label' column to df_scores so taht we can join with the odds label later 
    df_scores['score.label'] = df_scores['home_goals'].astype(str) + ":" + df_scores['away_goals'].astype(str)

    # Return the processed scores DataFrame
    return df_scores
