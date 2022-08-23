import numpy as np
import pandas as pd
import re

import sswiki.constants as const


def extractSpeed(df, col_fr, col_to, pat, repl=None):
    """Extract measurement based on pattern from df['col_fr'] to df['col_to']
    with changes described in replacement

    Keyword arguments:
    df -- A panda data frame with at least two columns
    col_fr -- The name of the column to extract the measurement from
    col_to -- The name of the column to extract the measurement to
    pat -- regex pattern of the date format to search for in df['col_fr'].
        Requires named group [xxxx]
    repl -- A string representing the replacement format. [xxxx]

    Return:
    A pandas series with recognized measurements [xxx]]
    """
    ROUND_DP = 3
    num_cols = len(df.columns)
    df[col_fr] = df[col_fr].str.normalize('NFKD')

    df = pd.concat([df, df.loc[df[col_to].isna(), col_fr].
                    str.extract(pat, flags=re.IGNORECASE)], axis=1)

    if repl == 'k':
        if 'kqd' in df.columns:
            df.loc[~df['kq'].isna() & df['kqd'].isna(), 'kqd'] = 0
            df['extract'] = df['kq'].astype(float) + \
                df['kqd'].astype(float) / 10

        else:
            df['extract'] = df['kq'].astype(float)

    elif repl == 'm':
        if 'kqd' in df.columns:
            df.loc[~df['kq'].isna() & df['kqd'].isna(), 'kqd'] = 0
            df['extract'] = df['kq'].astype(float) + \
                df['kqd'].astype(float) / 10

        else:
            df['extract'] = df['kq'].astype(float)

        df['extract'] = df['extract'] * const.MPH_TO_KNTS

    df[col_to] = np.where(df['extract'].isna(),
                          df[col_to],
                          np.round(df['extract'], ROUND_DP))
    df.drop(df.columns[[*range(num_cols, len(df.columns))]],
            axis=1, inplace=True)

    return df


def seriesToKnots(sf):
    """Convert mix of speed measure formats in a pandas Series to knots.

    Uses the first match in each data row. Where there is more than one number
    in each row, the Wikipedia data first number is typically [xxx].


    Keyword arguments:
    sf -- A panda series to convert

    Return:
    A pandas series with recognized measures as knots.
    """

    start_pat = r'(?P<SP>'
    qty_pat = r'(?P<kq>\d+())'
    qty_pat = r'(?P<kq>\d+)(?:\.(?P<kqd>\d))?'
    knots_pat = r'(?P<kn>\s?\+?(\[\d\])?\s?(?:(?:knots)|(?:kn)|(?:kt)))'
    mph_pat = r'(?P<kn>\+? (?:(?:miles per hour)|(?:mph)))'

    end_pat = r')'

    # for testing
    df = pd.DataFrame({'sf': sf})
    df['knots'] = np.nan

    # dd.d knots
    pat = start_pat + qty_pat + knots_pat + end_pat
    repl = 'k'
    df = extractSpeed(df, 'sf', 'knots', pat, repl)

    # dd.d miles per hours
    pat = start_pat + qty_pat + mph_pat + end_pat
    repl = 'm'
    df = extractSpeed(df, 'sf', 'knots', pat, repl)

    # dd.d k
    pat = start_pat + qty_pat + r'\s?k' + end_pat
    repl = 'k'
    df = extractSpeed(df, 'sf', 'knots', pat, repl)

    # dd.d
    pat = start_pat + qty_pat + end_pat
    repl = 'k'
    df = extractSpeed(df, 'sf', 'knots', pat, repl)

    return df['knots']
