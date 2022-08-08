import numpy as np
import pandas as pd


def extractHullNo(df, col_fr, col_ht, col_hn, pat):
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

    num_cols = len(df.columns)
    df[col_fr] = df[col_fr].str.normalize('NFKD')

    df = pd.concat([df, df.loc[df[col_hn].isna(), col_fr].
                    str.extract(pat)], axis=1)

    df[col_hn] = np.where(df['hn'].isna(), df[col_hn], df['hn'])
    df[col_ht] = np.where(df['ht'].isna(), df[col_ht], df['ht'])

    df.drop(df.columns[[*range(num_cols, len(df.columns))]],
            axis=1, inplace=True)

    return df


def seriesToHullNo(df, col_fr):
    """Convert mix of speed measure formats in a pandas Series to knots.

    Uses the first match in each data row. Where there is more than one number
    in each row, the Wikipedia data first number is typically [xxx].


    Keyword arguments:
    sf -- A panda series to convert

    Return:
    A pandas series with recognized measures as knots.
    """

    df['Hull_type'] = np.nan
    df['Hull_no'] = np.nan

    # (XXX-DDD)
    pat = r'\((?P<ht>[a-zA-Z]+)\-?(?P<hn>\d+)\)$'
    df = extractHullNo(df, col_fr, 'Hull_type', 'Hull_no', pat)

    # (YYYY)
    pat = r'\((?P<ht>)(?P<hn>\d{4})\)$'
    df = extractHullNo(df, col_fr, 'Hull_type', 'Hull_no', pat)

    # (XXX(X)-DDD)
    pat = r'\((?P<ht>[a-zA-Z]+\(\w+\))\-?(?P<hn>\d+)\)$'
    df = extractHullNo(df, col_fr, 'Hull_type', 'Hull_no', pat)

    # XXX-DDD
    pat = r'(?P<ht>[a-zA-Z]+)\-?(?P<hn>\d+)$'
    df = extractHullNo(df, col_fr, 'Hull_type', 'Hull_no', pat)

    # (XXX-DDD-X)
    pat = r'\((?P<ht>[a-zA-Z]+)\-?(?P<hn>\d+)\-?\w+\)$'
    df = extractHullNo(df, col_fr, 'Hull_type', 'Hull_no', pat)

    return df
