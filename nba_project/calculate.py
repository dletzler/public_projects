"""
Module for performing statistical measures of player consistency
"""

import numpy as np
import pandas as pd
from scipy import stats

def measure_group_consistency(df, group):
    """
    Returns overall consistency score for each group of stats
    :param df: (df)
    :param group: (str)
    :return score: (float)
    """
    if group == 'player_season':
        year = df['year'].tolist()[0]
        base = get_games_from_year(year)
    elif group == 'sum_season':
        df = df.groupby('year').sum().reset_index()
        base = 10
    else:
        years = list(set(df['year']))
        total_career_games_possible = sum([get_games_from_year(x) for x in years])
        base = max(total_career_games_possible, 820)
    results = {}
    for stat in [*norm_factor]:
        results['std_{stat}'.format(stat=stat)] = measure_vector_deviation(df[stat].values)
        results['ent_raw_{stat}'.format(stat=stat)] = measure_vector_entropy(df[stat].values, base)
        results['ent_norm_{stat}'.format(stat=stat)] =  results['ent_raw_{stat}'.format(stat=stat)]**norm_factor[stat]
    results['entropy_score'] = np.nanmean([value for key, value in list(results.items()) if key.startswith('ent_norm_')])**2
    results['deviation_score'] = np.nanmean([value for key, value in list(results.items()) if key.startswith('std_')])
    results['consistency_score'] = np.nanmean([results['entropy_score'], results['deviation_score']])
    score = pd.Series(results)
    return score


def get_games_from_year(year):
    """
    Returns the mininum number of team games/season for each year.
    :param year: (int)
    :return games: (int)
    """
    if year == 2021:
        games = 72
    elif year == 2020:
        games = 70
    elif year == 2012:
        games = 66
    elif year == 1999:
        games = 50
    elif year > 1967:
        games = 82
    elif year == 1967:
        games = 81
    elif year >= 1962:
        games = 80
    elif year == 1961:
        games = 79
    elif year == 1960:
        games = 75
    elif year >= 1954:
        games = 72
    elif year == 1953:
        games = 69
    elif year >= 1951:
        games = 66
    elif year == 1950:
        games = 62
    elif year == 1949:
        games = 60
    elif year == 1948:
        games = 48
    else:
        games = 61
    return games


def measure_vector_entropy(vec, base):
    """
    Converts vector of values into entropy score
    :param vec: (array)
    :param base: (int)
    :return entropy: (float)
    """
    adj_base = max(base, len(vec))
    vec_fil = vec[~np.isnan(vec)]
    vec_total = np.sum(vec_fil)
    vec_prob = vec_fil/vec_total
    entropy = stats.entropy(vec_prob, base=adj_base) if len(vec_prob) > 0 else np.nan
    return entropy


def measure_vector_deviation(vec):
    """
    Returns the complement of a value's weighted variance
    :param vec: (arr)
    :return std_score: (float)
    """
    the_max = np.nanmax(vec)/2
    std = np.nanstd(vec)
    std_norm = std/the_max 
    std_score = 1 - std_norm
    return std_score


def measure_vector_uniqueness(vec):
    """
    Returns percentage of values that are unique
    :param vec: (array)
    :return unique_score: (float)
    """
    unique = len(set(vec))
    unique_score = 1 - (unique - 1)/len(vec)
    return unique_score


norm_factor = {
    'trb': 3,
    'pts': 3.25,
    'ast': 2,
}
