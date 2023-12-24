# NHL PBP Data Scraper

## Version: 1.0.0

## Developer: Zach Andrews (StatsByZach)

### Overview

This Python program is designed to scrape and clean NHL play-by-play (PBP) data from the *NEW* NHL API. The project is maintained by Zach Andrews, known as @StatsByZach on X.

### Dependencies

- pandas
- requests
- numpy
- logging
- BeautifulSoup (bs4)

### Configuration

The program uses a configuration file named `scraper.log` for logging. The log records information about the program's execution, including any errors encountered during API requests.

### Usage

The main functionality of the scraper is to retrieve NHL PBP data for specific dates. The primary functions are:

1. `scrape_date(date)`: Scrapes games from a given date using the NHL API schedule endpoint. Returns a Pandas DataFrame of all plays from every game on the given day.
2. `scrape_game(game_id)`: Scrapes PBP data for a specific game using the NHL API gamecenter endpoint. Returns a Pandas DataFrame of all plays from the given game.

### How to Run

1. Install the required dependencies using `pip install -r requirements.txt`.
2. In another file in the same directory, add `import nhl_pbp_data_scraper as nhlpbpds` in requirments section.
3. Call either the `nhlpbpds.scrape_game(game_id)` or the `nhlpbpds.scrape_date(date)` function.

### Examples

```python
# Example usage to scrape data for a single game
game_id = 2023020300
single_game_pbp = nhlpbpds.scrape_game(2023020300)
print(single_game_pbp.head())

# Example usage to scrape data for a specific date
date = "2023-12-20"
date_pbp = nhlpbpds.scrape_date(date)
print(date_pbp.head())
```
Also see the examples.ipynb file to see these examples
### Column Documentation

1. **game_id**: Identifier for the game.
2. **game_season**: Season of the game.
3. **game_date**: Date of the game.
4. **game_type**: Type of the game (1 = Pre-season, 2 = Regular season, 3 = Playoffs)
5. **venue**: Venue where the game is played.
6. **home_team**: Home team participating in the game.
7. **away_team**: Away team participating in the game.
8. **home_coach**: Coach of the home team.
9. **away_coach**: Coach of the away team.
10. **period**: Current period of the game.
11. **period_type**: Type of the period (e.g., regulation, overtime).
12. **period_minutes_elapsed**: Minutes elapsed in the current period.
13. **period_seconds_elapsed**: Seconds elapsed in the current period.
14. **game_minutes_elapsed**: Total minutes elapsed in the game.
15. **game_seconds_elapsed**: Total seconds elapsed in the game.
16. **event**: Type of event (e.g., shot, goal).
17. **event_team**: Team associated with the event.
18. **shot_type**: Type of shot taken.
19. **description**: Description of the event.
20. **x_coordinate**: X-coordinate of the event on the rink.
21. **y_coordinate**: Y-coordinate of the event on the rink.
22. **zone**: Zone on the rink where the event occurred.
23. **is_shot_on_empty_net_rel**: Indicator if the shot was taken on an empty net relative to the shooting team.
24. **home_team_def_side**: Defensive side of the home team.
25. **home_score**: Score of the home team.
26. **away_score**: Score of the away team.
27. **event_p1_name**: Name of the primary event player.
28. **event_p2_name**: Name of the secondary event player.
29. **event_p3_name**: Name of the tertiary event player.
30. **situation_code**: Code representing the game situation. 4 digit code. formatted as number of away goalies on ice, number of away skaters on ice, number of home skaters on ice, number of home goalies on ice. So 1551 would be 5v5 with both goalies in net.
31. **home_skaters_on_ice**: Number of home team skaters on the ice.
32. **away_skaters_on_ice**: Number of away team skaters on the ice.
33. **home_goalie_on_ice**: Goalie of the home team on the ice.
34. **away_goalie_on_ice**: Goalie of the away team on the ice.
35. **strength**: Strength of the teams on the ice.
36. **strength_rel**: Strength of the teams on the ice relative to the event team. So 5v4 would mean event team has a man advantage, 4v5 would mean event team is short handed. 
37. **strength_cat_rel**: Category of strength_rel. Can be even, advantage, or short-handed, again relative to the event team.
38. **home_skater1 to home_skater6**: Players on the ice for the home team.
39. **home_goalie**: Goalie on the ice for the home team.
40. **away_skater1 to away_skater6**: Players on the ice for the away team.
41. **away_goalie**: Goalie on the ice for the away team.


