from datetime import datetime
import numpy as np
import pandas as pd
import re
import uuid

import sswiki.utils as utils
import sswiki.constants as const


def extractDate(df, col_fr, col_to, pat, repl=None):
    """Extract date based on pattern from df['col_fr'] to df['col_to'] with
    changes described in replacement

    Keyword arguments:
    df -- A panda data frame with at least two columns
    col_fr -- The name of the column to extract the dates from
    col_to -- The name of the column to extract the dates to
    pat -- regex pattern of the date format to search for in df['col_fr'].
        Requires named group "Date" if repl is None. Else requires named groups
        for "Day", "Month" and "Year" depending on what is passed by 'repl'
    repl -- A tuple of strings representing the replacement date format. Tuple
        is in the format (dd, mm, yyyy) Use "D" for day match from `pat`, or
        an integer to for a day of the month. Similarily, use "M" or [1 - 12]
        for month, and "Y" or a four digit integer for year.

    Return:
    A pandas series with recognized dates as datetime objects.
    """
    cols_in = df.columns

    df[col_fr] = df[col_fr].str.normalize('NFKD')

    if repl is None:
        df['extract'] = df.loc[df[col_to].isna(), col_fr].\
            str.extract(pat, flags=re.IGNORECASE)['Date']
    else:
        df = pd.concat([df, df.loc[df[col_to].isna(), col_fr].
                        str.extract(pat, flags=re.IGNORECASE)], axis=1)

        day = repl[0] if repl[0] != 'D' else df['Day']

        if repl[1] != 'M':
            month = datetime(1970, int(repl[1]), 1).strftime("%B")
        else:
            month = utils.lengthenMonth(df['Month'])

        year = "".join(repl[2:]) if repl[2] != 'Y' else df['Year']

        df['extract'] = day + " " + month + " " + year

    df[col_to] = np.where(df['extract'].isna(), df[col_to], df['extract'])
    df.drop(columns=df.columns.difference(cols_in), inplace=True)

    return df


def findDates(df, col_fr, col_to=None, pat=None):
    """Search for a pattern and extract the date when a match is found.

    For example, to search for "Scrapped on 4 Nov 1995", pass in the regex to
    match "Scrapped on" and the function will match on the `prefix_match` and
    one of the date patterns from `DATE_PATS` and will return the date in
    datetime format.

    Keyword arguments:
    df -- A panda data frame with the column to search
    col_fr -- The string column name to search for the match
    col_to -- The string column name to extract the date to. If `None`, then
        result will be stored in the `col_fr` column name
    pat -- A regex pattern to search for. If `None`, then only looks for a
        date pattern

    Return:
    A panda data frame with recognized dates as datetime objects.
    """
    RAND_COL_NAME = '__temp_col__' + uuid.uuid4().hex + uuid.uuid4().hex + '__'
    if (col_to is None) or col_to == col_fr:
        col_to = RAND_COL_NAME

    df[col_to] = np.nan

    # Iterate through each date pattern
    # Correct all dates to 'DD MMMMMM YYYY'
    for index, row in const.DATE_PAT_REPL.iterrows():
        if pat is None:
            pat_date = const.DATE_PATS.get(row['pat'])
        else:
            pat_date = pat + r'.*?' + const.DATE_PATS.get(row['pat'])

        df = extractDate(df, col_fr, col_to, pat_date, row['repl'])

    # Change to datetime
    df[col_to] = pd.to_datetime(df[col_to],
                                errors='coerce',
                                infer_datetime_format=True)

    if col_to == RAND_COL_NAME:
        df[col_fr] = df[col_to]
        df.drop(columns=col_to, inplace=True)

    return df
