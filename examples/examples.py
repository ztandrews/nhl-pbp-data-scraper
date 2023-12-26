################### examples.py ###################
#                                                 #
#            Quick program to provide             #
#            Examples of the scraper in use.      #
#            Run in project's root dir, using     #
#            python -m examples.ex                #
#                                                 #
###################################################


# Import modules
from scraper import nhl_pbp_data_scraper as nhlpbpds
import pandas

# Scrape a single game
game_id = 2023020350
single_game_pbp = nhlpbpds.scrape_game(game_id)
print(single_game_pbp.head())

# Scrape all games from a given date
date = "2023-12-20"
date_pbp = nhlpbpds.scrape_date(date)
print(date_pbp.head())

# If you prefer to have the data in a csv, you can use pd.to_csv to save each df after scraping, or just directly call it like this
nhlpbpds.scrape_game(game_id).to_csv("game_pbp.csv")

# If you're interested in doing some analysis to a df, go wild. Here are some basic examples for beginners

# All goals from a single game
goals = single_game_pbp.loc[single_game_pbp['event']=='goal']
print(goals[['event','event_team','event_primary_player','description']])
