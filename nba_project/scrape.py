"""
Module for scraping data from basketball-reference.com
"""

import copy
import datetime as dt
import numpy as np
import os
import pandas as pd
import pickle
import re
import requests as rq
import string
import time
from multiprocessing import Pool as ThreadPool

from bs4 import BeautifulSoup


def get_all_player_records():
    """
    Scrapes a master dictionary of all player gamelogs from Basketball-Reference
    :return all_players: (dict)
    """
    all_players = get_player_index()
    for key, value in list(all_players.items()):
        # If we have an incomplete pickle, we don't need to query again.
    	if not all_players[key].get('gamelog'):
            print("Fetching {} at {}".format(value['player_name'], dt.datetime.now()))
            years_active = range(value['start_year'], value['end_year'] + 1)
            all_players[key]['urls'] = {year: copy.copy(player_gamelog_master) \
                                                  .format(letter=value['first_letter'],
                                                          player=key,
                                                          year=year) for year in years_active}
            all_players[key]['gamelog'] = get_player_gamelogs(all_players[key]['urls'])
    save_to_pickle(all_players)
    all_players = try_missing_records_again(all_players)
    save_to_pickle(all_players)
    return all_players


def get_player_index():
    """
    Loads the index of all player gamelog URLs
    :return all_players: (dict)
    """
    print("Fetching the player indices at {}".format(dt.datetime.now()))
    if raw_save_file in os.listdir():
        with open(raw_save_file, 'rb') as fpath:
            all_players = pickle.load(fpath)
        print("Loaded index from pickle")
    else:
        all_players = {}
        for letter in string.ascii_lowercase:
            letter_players = get_player_page_indices(letter)
            all_players.update(letter_players)
            time.sleep(1)
        with open(raw_save_file, 'wb') as fpath:
            pickle.dump(all_players, fpath)
        print("Found {} players".format(len(all_players)))
    return all_players


def get_player_page_indices(letter):
    """
    Returns a dictionary that scrapes the player index on Basketball-Reference.com for 
    a given letter of the alphabet.
    :param url: (str)
    :return players: (dict)
    The record is indexed to player name and records necessary info for getting game logs:
    1. Player homepage
    2. Player start year
    3. Player end year
    """
    index_url = copy.deepcopy(player_url_master).format(letter=letter)
    player_url = copy.deepcopy(player_page_master)
    response = rq.get(index_url, timeout=5)
    text_bs = BeautifulSoup(response.text, 'html.parser')
    table = text_bs.find_all('tbody')
    rows = table[0].find_all('tr')
    players = {}
    for row in rows:
        page = row.find_all('a')[0]['href']
        ids = re.match(player_url, page) 
        player = ids[2]
        players[player] = {}
        players[player]['first_letter'] = ids[1]
        players[player]['player_name'] = row.find_all('a')[0].get_text()
        players[player]['start_year'] = int(row.find_all('td')[0].get_text())
        players[player]['end_year'] = int(row.find_all('td')[1].get_text())
    return players


def get_player_gamelogs(gamelog_urls):
    """
    Returns dict of {player: {year: {game: stats}}}
    :param gamelog_urls: (doct)
    :return gamelog_dict: (dict)
    This function uses multiprocessing to accelerate the query process
    """
    gamelog_dict = {}
    gamelog_tups = list(gamelog_urls.items())
    pool = ThreadPool(6)
    gamelogs = pool.map(get_year_gamelog, gamelog_tups)
    pool.close()
    pool.join()
    for year, year_dict in gamelogs:
        gamelog_dict[year] = year_dict
    return gamelog_dict


def get_year_gamelog(tup):
    """
    Year-level query syntax with simple error handling
    :param url: (str)
    :return year_dict: (dict)
    """
    year = tup[0]
    url = tup[1]
    try:
        year_dict = {}
        response = rq.get(url, timeout=5)
        text_bs = BeautifulSoup(response.text, 'html.parser')
        table = text_bs.find_all('tbody')
        if table:
            rs_rows = table[0].find_all('tr')
            year_dict = extract_game_stats(rs_rows, year)
        time.sleep(0.75)
    except Exception as e:
        print(e)
        year_dict = {'error': e}
        time.sleep(360)
    return year, year_dict


def extract_game_stats(rows, year):
    """
    Extracts stats from player's season gamelog
    :param rows: (list of bs4)
    :param gamelog_dict: (dict)
    :param year: (int)
    :return year_dict: (dict)
    """
    year_dict = {}
    for row in rows:
        if (row.get('id') or '').startswith('pgl_basic'):
            date = row.select('td[data-stat="date_game"]')[0].get_text()
            year_dict[date] = {}
            stats = [x.get('data-stat') for x in row.find_all('td') if x.get('data-stat')]
            for stat_name in stats:
                stat = row.select('td[data-stat="{stat_name}"]'.format(stat_name=stat_name))
                stat = np.nan if not stat[0].get_text() else stat[0].get_text()
                if stat_name == 'mp':
                    time_split = stat.split(':') if isinstance(stat, str) else None
                    minutes, seconds = (np.nan, np.nan) if not time_split \
                        else (float(time_split[0]), float(time_split[1]))
                    stat = minutes + seconds/60
                year_dict[date][stat_name] = stat
    return year_dict


def try_missing_records_again(all_players):
    """
    Looks for years that failed to scrape, then loops retries until the data is successfully queried.
    :param all_players: (dict)
    :return all_players: (dict)
    We stop after 4 failures to avoid an endless loop
    """
    for key in [*all_players]:
        years = [*all_players[key]['gamelog']]
        for year in years:
            if all_players[key]['gamelog'][year].get('error'):
                attempt = 1
                while all_players[key]['gamelog'][year].get('error') and attempt < 5:
                    tup = (year, all_players[key]['urls'][year])
                    all_players[key]['gamelog'][year] = get_year_gamelog(tup)[1]
                    attempt += 1
    return all_players


def save_to_pickle(all_players):
    """
    Pickles the output
    :param all_players: (dict)
    """
    with open(raw_save_file, 'wb') as fpath:
        pickle.dump(all_players, fpath)


def convert_dict_to_df(all_players):
    """
    Converts the nested dictionary of game logs into a dataframe
    :param all_players: (dict)
    :return player_df: (df)
    """
    player_df = pd.DataFrame({(i, j, k): all_players[i]['gamelog'][j][k]
                           for i in list(all_players.keys()) 
                           for j in list(all_players[i]['gamelog'].keys())
                           for k in list(all_players[i]['gamelog'][j].keys())
              }).transpose()
    player_df = player_df.reset_index().rename(columns={'level_0': 'player_key',
                                                    'level_1': 'year',
                                                    'level_2': 'game_date'})
    for stat in stats_to_measure:
        player_df[stat] = player_df[stat].apply(float)
    player_df['name'] = player_df['player_key'].apply(lambda x: all_players[x]['player_name'])
    player_df = player_df.loc[player_df.year <= 2021, :]
    return player_df


player_url_master = 'https://www.basketball-reference.com/players/{letter}/'
player_page_master = '/players/([a-z])/([A-Za-z0-9]+).html'
player_gamelog_master = 'https://www.basketball-reference.com/players/{letter}/{player}/gamelog/{year}'
raw_save_file = 'player_data.pickle'
stats_to_measure = ['gs', 'mp', 'fg', 'fga', 'fg_pct', 'fg3', 'fg3a',
       'fg3_pct', 'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl',
       'blk', 'tov', 'pf', 'pts', 'game_score', 'plus_minus']
