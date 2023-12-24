######################################### nhl_pbp_data_scraper.py ###########################################
#                                                                                                      #
#                                      Name: NHL PBP Data Scraper                                      #
#                                         V: 1.0.0                                                     #
#                                       Dev: Zach Andrews                                              #
#                                     About: This is a Python program to scrape and clean NHL          #
#                                            play-by-play data. The project is built by Zach           #
#                                            Andrews, aka @StatsByZach.                                #
#                                                                                                      #
########################################################################################################

######################################### Import Modules ###############################################
import pandas as pd
import requests
import numpy as np
import logging
from bs4 import BeautifulSoup as bs
############################################# Config ###################################################
logging.basicConfig(filename='scraper.log',level=logging.INFO, format='%(asctime)s - %(levelname)s  - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
logging.info("live_scraper.py program started")

# Function to scrape games from a certain date
def scrape_date(date):
    print("Scraping games from {}...\n".format(date))
    logging.info("scrape_date function called. Scraping games from {}...".format(date))
    # Make request to days schedule. Example URL: https://api-web.nhle.com/v1/schedule/2023-11-30
    try:
        req = requests.get("https://api-web.nhle.com/v1/schedule/{}".format(date))
        logging.info("Schedule API request status: {}".format(req))
        req.raise_for_status() 
    # Handle any exception related to the request
    except requests.exceptions.RequestException as req_exc:
        logging.error(f"Play-by-Play API request failed: {req_exc}")
    # Handle HTTP errors
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Play-by-Play API HTTP error occurred: {http_err}")
    # Handle value-related issues
    except ValueError as val_err:
        logging.error(f"Play-by-Play API Value error occured: {val_err}")
    else:
        games = pd.json_normalize(req.json()['gameWeek'][0]['games'])['id'].tolist()
        pbp_df = pd.DataFrame()
        for game in games:
            game_pbp = scrape_game(game)
            pbp_df = pd.concat([pbp_df,game_pbp])
    logging.info("scrape_date function finished.")
    return pbp_df

# Function to scrape single game
def scrape_game(game_id):
    print("Scraping game {}...".format(game_id))
    logging.info(" ********************* scrape_game function called. Scraping game {}... *********************".format(game_id))
    # Make request. New URL example is https://api-web.nhle.com/v1/gamecenter/2023020061/play-by-play
    try:
        req = requests.get("https://api-web.nhle.com/v1/gamecenter/{}/play-by-play".format(game_id))
        logging.info("Play-by-Play API request status: {}".format(req))
        req.raise_for_status() 
    # Handle any exception related to the request
    except requests.exceptions.RequestException as req_exc:
        logging.error(f"Play-by-Play API request failed: {req_exc}")
    # Handle HTTP errors
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Play-by-Play API HTTP error occurred: {http_err}")
    # Handle value-related issues
    except ValueError as val_err:
        logging.error(f"Play-by-Play API Value error occured: {val_err}")
    else:
         # Get PBP and transform into df
        # Full df is the entire json resp. includes some general game info
        full_df = pd.json_normalize(req.json())
        # pbp df is every event in the game
        pbp = pd.json_normalize(full_df['plays'][0])
        # players df is every player that played in the game
        players_df = pd.json_normalize(full_df['rosterSpots'][0])
        # teams
        teams = get_teams(full_df)
        # Add misc. info to pbp
        pbp = add_misc_info(pbp,full_df,game_id)
        # Clean each df
        # Clean players
        players_df = clean_players(players_df,teams)
        # Clean pbp
        pbp = clean_pbp(pbp,players_df,teams)
        logging.info("********************* scrape_game function finished for game {}. *********************".format(game_id))
        print("Game {} finished.\n".format(game_id))
    return pbp

# Add in lots of misc info that we need to get from not just the pbp frame. Coaches, etc
def add_misc_info(pbp,full_df,game_id):
    logging.info("add_misc_info function called...")
    #For tons more of misc info not on the regualr pbp endpoint go to https://api-web.nhle.com/v1/gamecenter/2022030237/landing
    try:
        req = requests.get("https://api-web.nhle.com/v1/gamecenter/{}/landing".format(game_id))
        logging.info("Gamecenter API request status: {}".format(req))
        req.raise_for_status() 
    except requests.exceptions.RequestException as req_exc:
        logging.error(f"Gamecenter API request failed: {req_exc}")
    # Handle HTTP errors
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Gamecenter API HTTP error occurred: {http_err}")
    # Handle value-related issues
    except ValueError as val_err:
        logging.error(f"Gamecenter API Value error occured: {val_err}")
    else:
        gamecenter = req.json()
        pbp['home_team'] = gamecenter['homeTeam']['abbrev']
        pbp['away_team'] = gamecenter['awayTeam']['abbrev']
        pbp['season'] = gamecenter['season']
        pbp['game_type'] = gamecenter['gameType']
        pbp['game_date'] = gamecenter['gameType']
        pbp['venue'] = gamecenter['venue']['default']
        pbp['home_coach']  = gamecenter['summary']['gameInfo']['homeTeam']['headCoach']['default'].upper()
        pbp['away_coach'] = gamecenter['summary']['gameInfo']['awayTeam']['headCoach']['default'].upper()
        pbp['game_id'] = game_id
        logging.info("add_misc_info function finished.")
    return pbp

# Function to get home and away team
def get_teams(full_df):
    logging.info("get_teams function called...")
    teams = {}
    home_id = full_df.iloc[0]['homeTeam.id']
    home_abv = full_df.iloc[0]['homeTeam.abbrev']
    away_id = full_df.iloc[0]['awayTeam.id']
    away_abv = full_df.iloc[0]['awayTeam.abbrev']
    teams[home_id]=home_abv
    teams[away_id]=away_abv
    logging.info("get_teams function finished.")
    return teams

# Function to clean the pbp df that will return
def clean_pbp(pbp_df,players_df,teams):
    logging.info("clean_pbp function called...")
    # Add p1 id, p2 id, p3 mid
    pbp_df = add_event_players(pbp_df,players_df)
    # Add teams
    pbp_df = add_event_team(pbp_df,teams,players_df)
    # Add total goals
    pbp_df = add_total_goals(pbp_df)
    #Parse situation doe
    pbp_df = parse_situation_code(pbp_df)
    # Add total elapsed time
    pbp_df = add_elapsed_time(pbp_df)
    #Add html pbp data. Need this for description and which players were on ice for each event
    pbp_df = add_html_report_data(pbp_df,players_df)
    #Add shootout logic. Do this because shootouts provde some intersting challenges
    pbp_df = add_shootout_logic(pbp_df)
    #Clean columns
    pbp_df = clean_columns(pbp_df)
    logging.info("clean_pbp function finished.")
    return pbp_df

# Function to clean the players df to be used throughout the scraper
def clean_players(players_df,teams):
    logging.info("clean_players function called...")
    players_df['team'] = players_df['teamId'].map(teams)
    players_df['player'] = players_df['firstName.default'].str.upper() + " " + players_df['lastName.default'].str.upper()
    players_df = players_df[['player',"playerId","team","teamId","sweaterNumber","positionCode"]]
    players_df = players_df.rename(columns={"playerId":"player_id","teamId":"team_id","sweaterNumber":"sweater_number","positionCode":"position"})
    logging.info("clean_players function finished.")
    return players_df

# Function to add players to the event action columns
def add_event_players(pbp_df,players_df):
    logging.info("add_event_players function called...")
    #Goals
    #frst, check if columns exist. There is no guarentee a non-so goal will be scored, and if thats the case
    # we will not have the assistPlayerId columns. So we have to check for that
    pbp_df = check_columns(pbp_df)
    pbp_df.loc[pbp_df['typeDescKey']=="goal",'event_primary_id'] = pbp_df['details.scoringPlayerId']
    pbp_df.loc[pbp_df['typeDescKey']=="goal",'event_secondary_id'] = pbp_df['details.assist1PlayerId']
    pbp_df.loc[pbp_df['typeDescKey']=="goal",'event_tertiary_id'] = pbp_df['details.assist2PlayerId']
    #SOGs, Misses, failed shots, blocks
    pbp_df.loc[(pbp_df['typeDescKey']=="shot-on-goal")|(pbp_df['typeDescKey']=="missed-shot")|(pbp_df['typeDescKey']=="failed-shot-attempt")|(pbp_df['typeDescKey']=="blocked-shot"),'event_primary_id'] = pbp_df['details.shootingPlayerId']
    #Hits
    pbp_df.loc[pbp_df['typeDescKey']=="hit","event_primary_id"] = pbp_df['details.hittingPlayerId']
    pbp_df.loc[pbp_df['typeDescKey']=="hit","event_secondary_id"] = pbp_df['details.hitteePlayerId']
    #Faceoffs
    pbp_df.loc[pbp_df['typeDescKey']=="faceoff","event_primary_id"] = pbp_df['details.winningPlayerId']
    pbp_df.loc[pbp_df['typeDescKey']=="faceoff","event_secondary_id"] = pbp_df['details.losingPlayerId']
    #Blocking player
    pbp_df.loc[pbp_df['typeDescKey']=="blocked-shot","event_secondary_id"] = pbp_df['details.blockingPlayerId']
    #Gives, takes
    pbp_df.loc[(pbp_df['typeDescKey']=="giveaway")|(pbp_df['typeDescKey']=="takeaway"),"event_primary_id"] = pbp_df['details.playerId']
    #Penalty
    pbp_df.loc[pbp_df['typeDescKey']=="penalty","event_primary_id"] = pbp_df['details.committedByPlayerId']
    pbp_df.loc[pbp_df['typeDescKey']=="penalty","event_secondary_id"] = pbp_df['details.drawnByPlayerId']
    #If its a team penalty, check served by player id?
    pbp_df.loc[(pbp_df['typeDescKey']=="penalty")&(pbp_df['details.servedByPlayerId'].isna()),"event_primary_id"] = pbp_df['details.committedByPlayerId']
    pbp_df.loc[(pbp_df['typeDescKey']=="penalty")&(pbp_df['details.servedByPlayerId'].isna()==False),"event_primary_id"] = pbp_df['details.servedByPlayerId']
    #Dict of games players
    games_players = dict(zip(players_df['player_id'], players_df['player']))
    pbp_df['event_primary_player'] = pbp_df['event_primary_id'].map(games_players)
    pbp_df['event_secondary_player'] = pbp_df['event_secondary_id'].map(games_players)
    pbp_df['event_tertiary_player'] = pbp_df['event_tertiary_id'].map(games_players)
    logging.info("add_event_players function finished.")
    return pbp_df

# Function to check if all detail columns are present
def check_columns(pbp_df):
    logging.info("check_columns function called...")
    cols = pbp_df.columns.tolist()
    needed = ['details.assist1PlayerId','details.assist2PlayerId','details.committedByPlayerId','details.drawnByPlayerId','details.servedByPlayerId',
              'details.blockingPlayerId','details.hittingPlayerId','details.hitteePlayerId','details.shootingPlayerId','details.scoringPlayerId',
              'details.playerId','details.winningPlayerId','details.losingPlayerId']
        # Find items in list1 but not in list2
    difference = set(needed) - set(cols)
    # Convert the result back to a list
    result = list(difference)
    pbp_df[result] = np.nan
    logging.info("check_columns function finished.")
    return pbp_df

# Function to add event team to df
def add_event_team(pbp_df,teams,players_df):
    logging.info("add_event_team function called...")
    # The pbp data gives the event owner ID to the blocker rather than the shooter for blocked shots. We need to change that
    players_dict = dict(zip(players_df['player_id'], players_df['team_id']))
    pbp_df['shootingPlayerTeam_temp'] = pbp_df['details.shootingPlayerId'].map(players_dict)
    pbp_df['details.eventOwnerTeamId'] = np.where(pbp_df['typeDescKey']=="blocked-shot",pbp_df['shootingPlayerTeam_temp'],pbp_df['details.eventOwnerTeamId'])
    # Now map
    pbp_df['event_team'] = pbp_df['details.eventOwnerTeamId'].map(teams)
    logging.info("add_event_team  function finished.")
    return pbp_df

# Function to add total goals at the time of each event
def add_total_goals(pbp_df):
    logging.info("add_total_goals function called...")
    pbp_df['isHomeGoal']=0
    pbp_df['isAwayGoal']=0
    pbp_df.loc[((pbp_df['typeDescKey']=="goal")&(pbp_df['event_team']==pbp_df['home_team'])),"isHomeGoal"] = 1
    pbp_df.loc[((pbp_df['typeDescKey']=="goal")&(pbp_df['event_team']==pbp_df['away_team'])),"isAwayGoal"] = 1
    pbp_df['away_score'] = pbp_df['isAwayGoal'].cumsum()
    pbp_df['home_score'] = pbp_df['isHomeGoal'].cumsum()
    pbp_df.loc[((pbp_df['typeDescKey']=="goal")&(pbp_df['event_team']==pbp_df['home_team'])),'home_score'] = pbp_df['home_score']-1
    pbp_df.loc[((pbp_df['typeDescKey']=="goal")&(pbp_df['event_team']==pbp_df['away_team'])),'away_score'] = pbp_df['away_score']-1
    logging.info("add_total_goals function finished.")
    return pbp_df

# Function to parse situation code
def parse_situation_code(pbp_df):
    logging.info("parse_situation_code function called...")
    # The situation code is a 4 digit number formatted as follows:
    # AG,AS,HS,HG
    # AG - Away gaolie on ice (1 yes, 0 no)
    # AS - Away skaters on ice (1,3,4,5,6)
    # HS - Home skaters on ice (1,3,4,5,6)
    # HG -Home goalie on ice (1 yes, 0 no)
    #Add in base case
    pbp_df.loc[pbp_df['situationCode'].isna(),'situationCode'] = "0000"
    pbp_df['home_goalie_on_ice'] = pbp_df['situationCode'].str[3].astype(int)
    pbp_df['away_goalie_on_ice'] = pbp_df['situationCode'].str[0].astype(int)
    pbp_df['home_skaters_on_ice'] = pbp_df['situationCode'].str[2].astype(int)
    pbp_df['away_skaters_on_ice'] = pbp_df['situationCode'].str[1].astype(int)
    # We can extract some good info from these
    pbp_df['strength'] = pbp_df['situationCode'].str[1] + "v" + pbp_df['situationCode'].str[2]
    pbp_df.loc[pbp_df['event_team']==pbp_df['home_team'],"strength_rel"] = pbp_df['situationCode'].str[2] + "v" + pbp_df['situationCode'].str[1]
    pbp_df.loc[pbp_df['event_team']==pbp_df['away_team'],"strength_rel"] = pbp_df['situationCode'].str[1] + "v" + pbp_df['situationCode'].str[2]
    pbp_df.loc[pbp_df['home_skaters_on_ice']==pbp_df['away_skaters_on_ice'],"strength_cat_rel"] = "even"
    # is advantage
    pbp_df.loc[(pbp_df['home_skaters_on_ice']>pbp_df['away_skaters_on_ice'])&(pbp_df['event_team']==pbp_df['home_team']),"strength_cat_rel"] = "advantage"
    pbp_df.loc[(pbp_df['away_skaters_on_ice']>pbp_df['home_skaters_on_ice'])&(pbp_df['event_team']==pbp_df['away_team']),"strength_cat_rel"] = "advantage"
    # is short handed
    pbp_df.loc[(pbp_df['home_skaters_on_ice']<pbp_df['away_skaters_on_ice'])&(pbp_df['event_team']==pbp_df['home_team']),"strength_cat_rel"] = "short-handed"
    pbp_df.loc[(pbp_df['away_skaters_on_ice']<pbp_df['home_skaters_on_ice'])&(pbp_df['event_team']==pbp_df['away_team']),"strength_cat_rel"] = "short-handed"
    # is penalty shot
    pbp_df.loc[(((pbp_df['situationCode'].astype(str)=="1010")|(pbp_df['situationCode'].astype(str)=="0101"))&((pbp_df['period']!=5)&(pbp_df['game_type']!=3))),"strength_cat_rel"] = "penalty-shot"
    pbp_df.loc[(((pbp_df['situationCode'].astype(str)=="1010")|(pbp_df['situationCode'].astype(str)=="0101"))&((pbp_df['game_type']==3))),"strength_cat_rel"] = "penalty-shot"
    # is shootout
    pbp_df.loc[(((pbp_df['situationCode'].astype(str)=="1010")|(pbp_df['situationCode'].astype(str)=="0101"))&((pbp_df['period']==5)&(pbp_df['game_type']!=3))),"strength_cat_rel"] = "shootout-shot"
    # We can make an is shot on empty net metric
    pbp_df.loc[(pbp_df['typeDescKey']=='shot-on-goal')|(pbp_df['typeDescKey']=="missed-shot")|(pbp_df['typeDescKey']=="blocked-shot")|(pbp_df['typeDescKey']=="goal"),"is_shot_on_empty_net_rel"] = 0
    pbp_df.loc[((pbp_df['situationCode'].str[0]=="0")&(pbp_df['event_team']==pbp_df['home_team'])&((pbp_df['typeDescKey']=='shot-on-goal')|(pbp_df['typeDescKey']=="missed-shot")|(pbp_df['typeDescKey']=="blocked-shot")|(pbp_df['typeDescKey']=="goal"))),"is_shot_on_empty_net_rel"] = 1
    pbp_df.loc[((pbp_df['situationCode'].str[3]=="0")&(pbp_df['event_team']==pbp_df['away_team'])&((pbp_df['typeDescKey']=='shot-on-goal')|(pbp_df['typeDescKey']=="missed-shot")|(pbp_df['typeDescKey']=="blocked-shot")|(pbp_df['typeDescKey']=="goal"))),"is_shot_on_empty_net_rel"] = 1
    logging.info("parse_situation_code function finished.")
    return pbp_df

# Function to get elapsed time from the timeInPeriod field
def add_elapsed_time(pbp_df):
    logging.info("add_elapsed_time function called...")
    pbp_df['period_seconds_elapsed'] = pbp_df['timeInPeriod'].str[:2].astype(int)*60 +  pbp_df['timeInPeriod'].str[3:].astype(int)
    pbp_df['period_minutes_elapsed'] = pbp_df['period_seconds_elapsed']/60
    pbp_df['game_seconds_elapsed'] = np.where(pbp_df['period']==1,pbp_df['period_seconds_elapsed'],(pbp_df['period_seconds_elapsed']+((pbp_df['period']-1)*1200)))
    pbp_df['game_minutes_elapsed'] = pbp_df['game_seconds_elapsed'] /60
    logging.info("add_elapsed_time function finished.")
    return pbp_df

# Function to add the HTML report to the df. The HTML report is very important as it's the easiest
# way to get each teams players on ice for each event
def add_html_report_data(pbp_df, players_df):
    logging.info("add_html_report function called...")
    # URL example: https://www.nhl.com/scores/htmlreports/20232024/PL010035.HTM
    season = pbp_df.iloc[0]['season']
    trimmed_game_id = str(pbp_df.iloc[0]['game_id'])[5:]
    try:
        req = requests.get("https://www.nhl.com/scores/htmlreports/{}/PL0{}.HTM".format(season,trimmed_game_id))
        logging.info("HTML Report request status: {}".format(req))
        req.raise_for_status() 
    except requests.exceptions.RequestException as req_exc:
        logging.error(f"HTML Report request failed: {req_exc}")
    # Handle HTTP errors
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTML Report HTTP error occurred: {http_err}")
    # Handle value-related issues
    except ValueError as val_err:
        logging.error(f"HTML Report Value error occured: {val_err}")
    else:
        # adding this here. The api recogized failed so attempts as their own event where as the html pbp does not. Making this simple change
        pbp_df.loc[pbp_df['typeDescKey']=="failed-shot-attempt",'typeDescKey'] = "missed-shot"
        html_doc = req.text
        soup = bs(html_doc, 'html.parser')
        # this is how we select the rows of the html report. They are all elements with the class *=bborder
        td_elements = soup.select('td[class*=bborder]')
        # td_elements is an array of each td object that we found
        # every 8 items in this array = 1 row on the html pbp report, so we need to group by every
        # 8 elements in the array using this logic
        chunk_size = 8
        rows = []
        for i in range(0, len(td_elements), chunk_size):
            chunk = td_elements[i:i + chunk_size]
            rows.append(chunk)
        df = pd.DataFrame()
        for row in rows:
            # Skip these events because they won't exist in the api data
            if row[4].text.strip() == "PGSTR" or row[4].text.strip()=="PGEND" or row[4].text.strip()=="ANTHEM" or row[4].text.strip() == "Event":
                continue
            else:
                # Initial assignments. Addying try catch in case of some html error
                try:
                    event_num = row[0].text.strip()
                    period = int(row[1].text.strip())
                    strength = row[2].text.strip()
                    time = row[3].text.strip()
                    event = row[4].text.strip()
                    description = row[5].text.strip()
                    away_players = row[6].select("font")
                    home_players = row[7].select("font")
                except ValueError:
                    logging.error("Error assigning a variable from an html report row")
                # Clean time because its formatted with elapsed/remaining combined. We will only use elapsed
                period_time_elapsed = clean_time(time)
                # Get the event owener from html report
                event_primary_player = extract_event_primary_player(event,description,players_df)
                if len(period_time_elapsed.split(":")[0])==1:
                    period_time_elapsed = "0"+period_time_elapsed
                d = {"event_num":event_num,"timeInPeriod":period_time_elapsed,"period":period,"strength":strength,'event':event,'description':description,'event_primary_player':event_primary_player}
                d = add_players(d,away_players,'away')
                d = add_players(d,home_players,'home')
                temp = pd.DataFrame([d])
                df = pd.concat([df,temp],ignore_index=True)
        df = add_elapsed_time(df)
        # We need to map the html pbp events to their corresponding api pbp event for joining purposes. Also need to re-assign a few events
        events_map = {"FAC":"faceoff",'SHOT':"shot-on-goal",'BLOCK':'blocked-shot','STOP':'stoppage','MISS':'missed-shot','HIT':'hit','TAKE':'takeaway','GIVE':"giveaway",
                      'GOAL':'goal','PSTR':'period-start','PENL':"penalty",'PEND':"period-end",'DELPEN':"delayed-penalty",'GEND':"game-end",'SOC':'shootout-complete',
                      "CHL":"stoppage",'EIEND':'stoppage','EISTR':'stoppage'}
        df['event'] = df['event'].map(events_map)
        if 'home_skater6' not in df.columns:
            df['home_skater6'] = np.nan
        if 'away_skater6' not in df.columns:
            df['away_skater6'] = np.nan
        df=df.reset_index()
        pbp_df=pbp_df.rename(columns={"typeDescKey":"event"})
        # We need to merge on these 3 things because:
        # - game seconds isnt enough because there can be multiple events in 1 second
        # - game and events arent enough because for penalties there can be multiple assigned in the same second
        # - we finally land on including the event player to distingusih events 
        pbp_df = pd.merge(pbp_df,df,on=['game_seconds_elapsed','event','event_primary_player'],how='inner')
        pbp_df = pbp_df.drop_duplicates(subset=['description','game_seconds_elapsed','event_primary_player'])
        pbp_df = pbp_df.drop(columns=["period_y",'strength_y','timeInPeriod_y', 'period_minutes_elapsed_y',  'game_minutes_elapsed_y', 'period_seconds_elapsed_y'])
        pbp_df = pbp_df.rename(columns={"period_x":'period','strength_x':"strength",'timeInPeriod_x':"timeInPeriod",'period_minutes_elapsed_x':'period_minutes_elapsed',
                                        'game_minutes_elapsed_x':'game_minutes_elapsed','period_seconds_elapsed_x':'period_seconds_elapsed'})
    logging.info("add_html_report function finished.")
    return pbp_df

# Func to clean the time that is on the html report
def clean_time(time):
    # Time in the html report is formated weird when scraped so we need to fix. Its time remaining and time elapsed combined. Ex - 20:000:00
    split_time = time.split(":")
    time_elapsed = split_time[0] + ":" + split_time[1][:2]
    return time_elapsed

# Function to add players in the html df
def add_players(event_dict,players_array,home_or_away):
    for i in range(1,len(players_array)+1):
        p = players_array[i-1]
        player = p.get("title",'')
        player_split_name = player.split(" - ")[1]
        player_split_pos = player.split(" - ")[0]
        if player_split_pos == "Goalie":
            skater = "{}_goalie".format(home_or_away)
        else:
            skater = '{}_skater{}'.format(home_or_away,i)
        event_dict[skater] = player_split_name
    return event_dict

# Fucntion to add shootout logic. So when I made the original total score function I forgot shootout goals would be counted as goals on pbp
# so we need this function to change shootout goals to a new event called shooutout-goals, and then calculate the new score.
# This will be skipped if there was no shootout in the game
def add_shootout_logic(pbp_df):
    logging.info("add_shootout_logic function called...")
    if "shootout-complete" in pbp_df['event'].value_counts().keys().tolist():
        # first, we have to adjust the score to not add up when a shootout goal is scored
        score_end_of_reg = pbp_df[(pbp_df['event']=="period-end")&(pbp_df['period']==4)]['home_score'].tolist()[0]
        pbp_df.loc[pbp_df['strength_cat_rel']=='shootout-shot','home_score'] = score_end_of_reg
        pbp_df.loc[pbp_df['strength_cat_rel']=='shootout-shot','away_score'] = score_end_of_reg
        # build logic
        pbp_df.loc[((pbp_df['strength_cat_rel']=="shootout-shot")&(pbp_df['event']=="goal")),'is_shootout_goal'] = 1
        pbp_df.loc[pbp_df['is_shootout_goal']==1,"event"] = "shootout-goal"
        #check who won the shootout
        # get home away
        home_team = pbp_df.iloc[0]['home_team']
        away_team = pbp_df.iloc[0]['away_team']
        so_goals = pbp_df[(pbp_df['is_shootout_goal']==1)]
        home_goals = len(so_goals[so_goals['event_team']==home_team])
        away_goals = len(so_goals[so_goals['event_team']==away_team])
        print()
        if home_goals > away_goals:
            pbp_df.loc[pbp_df['event']=='game-end','home_score'] = score_end_of_reg+1
            pbp_df.loc[pbp_df['event']=='game-end','away_score'] = score_end_of_reg
        else:
            pbp_df.loc[pbp_df['event']=='game-end','home_score'] = score_end_of_reg
            pbp_df.loc[pbp_df['event']=='game-end','away_score'] = score_end_of_reg+1
    else:
        pbp_df = pbp_df
    logging.info("add_shootout_logic function finished.")
    return pbp_df


# Function to get the event player from an html report row
def extract_event_primary_player(event,description,players_df):
    description = description.replace('\xa0', ' ')
    split_desc = description.split(" ")
    # If any f these events p1 will be NaN. Make sure to add those other weird events.
    # Adding in a try catch because sometimes the html pbp is strange when trying to live scrape
    try:
        if event in ['PGSTR','PGEND','ANTHEM','PSTR','GEND','STOP','PEND','DELPEN','SOC','CHL','EIEND','EISTR']:
            name=np.nan
        # These all look similar. TEAM NUBER LAST NAME
        elif event in ['HIT','MISS','BLOCK',"PENL",'GOAL']:
            team = split_desc[0]
            if "Served" in description:
                spl_des = description.split("By:")
                num_str = spl_des[1]
                num = num_str.split(" ")[1]
            else:
                num = split_desc[1]
            num = int(num.replace("#",''))
            name = players_df[(players_df['sweater_number']==num)&(players_df['team']==team)]['player'].tolist()[0]
        # These are all simar to. # TEAM EVENT - NUMBER LASTNAME
        elif event in ["GIVE","TAKE","SHOT"]:
            team = split_desc[0]
            num = split_desc[3]
            num = int(num.replace("#",''))
            name = players_df[(players_df['sweater_number']==num)&(players_df['team']==team)]['player'].tolist()[0]
        elif event == "FAC":
            team = split_desc[0]
            if split_desc[5]==team:
                num=split_desc[6]
            else:
                # Becasue joel erikson ek and the van riemsdyk exist
                split_again = description.split("vs ")
                split_again_2 = split_again[1].split(" ")[1]
                num=split_again_2
            num = int(num.replace("#",''))
            name = players_df[(players_df['sweater_number']==num)&(players_df['team']==team)]['player'].tolist()[0]
        else:
            name = np.nan
    except:
        name=np.nan
    return name

# Function to clean the columns 
def clean_columns(pbp_df):
    logging.info("clean_columns function called...")
    pbp_df = pbp_df.rename(columns={"situationCode":"situation_code","homeTeamDefendingSide":"home_team_def_side",
                                    "periodDescriptor.periodType":"period_type","details.xCoord":"x_coordinate",
                                    "details.yCoord":"y_coordinate","details.zoneCode":"zone","details.shotType":"shot_type",
                                    "season":"game_season"})
    
    pbp_df = pbp_df[['game_id',"game_season",'game_date',"game_type","venue","home_team","away_team","home_coach","away_coach","period",
                     'period_type',"period_minutes_elapsed","period_seconds_elapsed","game_minutes_elapsed","game_seconds_elapsed",
                     "event","event_team",'shot_type',"description",'x_coordinate','y_coordinate','zone','is_shot_on_empty_net_rel',
                     'home_team_def_side','home_score','away_score',"event_primary_player",'event_secondary_player','event_tertiary_player','situation_code',
                     'home_skaters_on_ice','away_skaters_on_ice','home_goalie_on_ice','away_goalie_on_ice','strength','strength_rel',
                     'strength_cat_rel','home_skater1', 'home_skater2','home_skater3', 'home_skater4', 'home_skater5','home_skater6',
                     'home_goalie','away_skater1', 'away_skater2', 'away_skater3', 'away_skater4','away_skater5','away_skater6','away_goalie']]
    logging.info("clean_columns function finished.")
    return pbp_df