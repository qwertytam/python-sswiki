import numpy as np
import pandas as pd
import re

import sswiki.constants as const


def extractMes(df, col_fr, col_to, pat, repl=None):
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
    df.fillna(value={'inq': 0}, inplace=True)

    repl = list(repl)
    if repl[0] == 'm':
        df['extract'] = df['mq']
    elif repl[0] == 'f':
        df['extract'] = np.round(
            df['ftq'].astype(float) * const.FT_TO_M, ROUND_DP)

    if len(repl) > 1 and repl[1] == 'i':
        df['extract'] = np.round(
            df['extract'] + df['inq'].astype(float) * const.IN_TO_M, ROUND_DP)

    df[col_to] = np.where(df['extract'].isna(), df[col_to], df['extract'])
    df.drop(df.columns[[*range(num_cols, len(df.columns))]],
            axis=1, inplace=True)

    return df


def seriesToMetres(sf):
    """Convert mix of linear measure formats in a pandas Series to metres.

    Keyword arguments:
    sf -- A panda series to convert`

    Return:
    A pandas series with recognized measures as metres.
    """

    start_pat = r'(?P<Mes>'
    m_pat = r'(?P<m>(?P<mq>\d+(?:\.\d+)?)\sm)'
    ft_pat = r'(?P<ft>(?P<ftq>\d+)(?P<ftd>\'|(?:\s?ft)|(?: feet)))'
    in_pat = r'(?P<in>(?P<inq>\d+)(?P<ind>\"|(?: in)))'
    end_pat = r')'

    # for testing
    df = pd.DataFrame({'sf': sf})
    df['metres'] = np.nan

    # Find any existing measurements in metres
    pat = start_pat + m_pat + end_pat
    repl = 'm'
    df = extractMes(df, 'sf', 'metres', pat, repl)

    # Correct feet and/or inches
    pat = start_pat + ft_pat + '( ' + in_pat + ')?' + end_pat
    repl = 'fi'
    df = extractMes(df, 'sf', 'metres', pat, repl)

    return df['metres']
