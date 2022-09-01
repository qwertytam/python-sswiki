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


def seriesToHullNo(df, col_fr, pats, ht_col, hn_col):
    """Extract vessel hull type and number from provided column

    Keyword arguments:
    df -- A panda data frame with column name provided by `col_fr`
    col_fr -- Column name with the Wikiepdia vessel article url
    pats -- List of regex patterns to use to extract. Should have group name
        `ht` for 'Hull_type' and `hn` for 'Hull_no'
    ht_col -- Column name to store hull type in
    hn_col -- Column name to store hull number in

    Return:
    A pandas data frame with additional columns `Hull_type` and `Hull_no`
    """
    for pat in pats:
        df = extractHullNo(df, col_fr, 'Hull_type', 'Hull_no', pat)

    return df
