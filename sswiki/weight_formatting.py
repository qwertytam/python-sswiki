import numpy as np
import pandas as pd
import re

import sswiki.constants as const


def extractWeight(df, col_fr, col_to, pat, repl=None):
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
    ROUND_DP = 0
    num_cols = len(df.columns)
    df[col_fr] = df[col_fr].str.normalize('NFKD')

    df = pd.concat([df, df.loc[df[col_to].isna(), col_fr].
                    str.extract(pat, flags=re.IGNORECASE)], axis=1)

    if repl == 'mt':
        df.loc[df['qk'].isna() & ~df['qh'].isna(), 'qk'] = 0
        df['extract'] = df['qk'].astype(float) * 1000 + df['qh'].astype(float)

    elif repl == 'lt':
        if 'qk' in df.columns:
            df.loc[df['qk'].isna() & ~df['qh'].isna(), 'qk'] = 0
            df['extract'] = df['qk'].astype(float) * 1000 + \
                df['qh'].astype(float)

        else:
            df['extract'] = df['qh'].astype(float)

        df['extract'] = df['extract'] * const.LTONS_TO_MTONS

    elif repl == 'st':
        df.loc[df['qk'].isna() & ~df['qh'].isna(), 'qk'] = 0
        df['extract'] = df['qk'].astype(float) * 1000 + df['qh'].astype(float)
        df['extract'] = df['extract'] * const.STONS_TO_MTONS

    df[col_to] = np.where(df['extract'].isna(),
                          df[col_to],
                          np.round(df['extract'], ROUND_DP))
    df.drop(df.columns[[*range(num_cols, len(df.columns))]],
            axis=1, inplace=True)

    return df


def seriesToTonnes(sf):
    """Convert mix of wieght measure formats in a pandas Series to metric
    tonnes.

    Uses the first match in each data row. Where there is more than one number
    in each row, the Wikipedia data first number is typically standard load
    displacment (for surface vessels) or surfaced displacement
    (for submarines).


    Keyword arguments:
    sf -- A panda series to convert

    Return:
    A pandas series with recognized measures as metric tonnes.
    """

    start_pat = r'(?P<WT>'
    qty_pat = r'(?:(?P<qk>\d{1,3})\,)?(?P<qh>\d{1,3})'

    # Assuming "t." abbreviated for metric tons
    # ref Wikipedia article on long ton
    metric_tons_pat = r'(?P<mt> (?:(?:metric tons)|(?:t(?:\.?))))'

    # tons is typicaly Naval abbreviation for long tons
    # ref Wikipedia article on long ton
    long_tons_pat = r'(?P<lt> (?:(t\.)?(?:long )|(?:\(long\) ))?ton(?:s)?)'

    short_tons_pat = r'(?P<st> short ton(?:s)?)'

    end_pat = r')'

    # for testing
    df = pd.DataFrame({'sf': sf})
    df['tonnes'] = np.nan

    # dd,ddd long tons
    pat = start_pat + qty_pat + long_tons_pat + end_pat
    repl = 'lt'
    df = extractWeight(df, 'sf', 'tonnes', pat, repl)

    # dd,ddd short tons
    pat = start_pat + qty_pat + short_tons_pat + end_pat
    repl = 'st'
    df = extractWeight(df, 'sf', 'tonnes', pat, repl)

    # dd,ddd metric tons
    pat = start_pat + qty_pat + metric_tons_pat + end_pat
    repl = 'mt'
    df = extractWeight(df, 'sf', 'tonnes', pat, repl)

    # dd,ddd >> assumed long tons
    pat = start_pat + qty_pat + r'.+ton(?:(?:.+)|$)' + end_pat
    repl = 'lt'
    df = extractWeight(df, 'sf', 'tonnes', pat, repl)

    # dd,ddd >> assumed long tons, excluding cubic ft or cubic metres
    pat = start_pat + qty_pat + r'(?: [^cm])' + end_pat
    repl = 'lt'
    df = extractWeight(df, 'sf', 'tonnes', pat, repl)

    # ddddd >> assumed long tons
    pat = r'^' + start_pat + r'(?P<qh>\d{2,})' + end_pat + r'$'
    repl = 'lt'
    df = extractWeight(df, 'sf', 'tonnes', pat, repl)

    return df['tonnes']
