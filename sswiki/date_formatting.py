from datetime import datetime
import numpy as np
import pandas as pd
import sswiki.utils as utils


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
    repl -- A string representing the replacement date format. Use "D" for day,
        "M" for month, and "Y" for year e.g. "DMY". If no day or month present
        in 'pat', can use integers 1 to 9 to represent dates e.g. 5 for May.

    Return:
    A pandas series with recognized dates as datetime objects.
    """
    num_cols = len(df.columns)
    df[col_fr] = df[col_fr].str.normalize('NFKD')

    if repl is None:
        df['extract'] = df.loc[df[col_to].isna(), col_fr].\
            str.extract(pat)['Date']
    else:
        df = pd.concat([df, df.loc[df[col_to].isna(), col_fr].
                        str.extract(pat)], axis=1)

        repl = list(repl)

        day = repl[0] if repl[0] != 'D' else df['Day']

        if repl[1] != 'M':
            # Obvisouly this doesn't work for any month >= 10!
            month = datetime(1970, int(repl[1]), 1).strftime("%B")
        else:
            month = utils.lengthenMonth(df['Month'])

        year = "".join(repl[2:]) if repl[2] != 'Y' else df['Year']

        df['extract'] = day + " " + month + " " + year

    df[col_to] = np.where(df['extract'].isna(), df[col_to], df['extract'])
    df.drop(df.columns[[*range(num_cols, len(df.columns))]],
            axis=1, inplace=True)

    return df


def seriesToDateTime(sf):
    """Convert mix of date formats in a pandas Series to datetime.

    Keyword arguments:
    sf -- A panda series to convert`

    Return:
    A pandas series with recognized dates as datetime objects.
    """

    start_pat = r'(?P<Date>'
    days_pat = r'(?P<Day>[1-3]?\d)'
    months_pat = r'\b(?P<Month>' + \
        'Jan(?:uary)?' + \
        '|Feb(?:ruary)?' + \
        '|Mar(?:ch)?' + \
        '|Apr(?:il)?' + \
        '|May(?:)?' + \
        '|Jun(?:e)?' + \
        '|Jul(?:y)?' + \
        '|Aug(?:ust)?' + \
        '|Sep(?:tember)?' + \
        '|Oct(?:ober)?' + \
        '|Nov(?:ember)?' + \
        '|Dec(?:ember)?' + ')'
    year_pat = r'(?P<Year>[1-2]\d{3})'
    end_pat = r')'

    ref_pat = r'(?:\s?\[\d\])*'
    ord_pat = r'(?:st|nd|rd|th)?'

    # for testing
    df = pd.DataFrame({'sf': sf})
    df['dt'] = np.nan

    # Correct all dates to 'DD MMMMMM YYYY'
    # Correct 'DD MMMMMM YYYY' with optional reference after 'DD' or 'MMMMMM'
    pat = start_pat + days_pat + ref_pat + ' ' \
        + months_pat + ref_pat + ' ' + year_pat + end_pat
    repl = 'DMY'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    # Correct 'MMMMMM DD, YYYY' with optional ordinal and reference after 'DD'
    pat = start_pat + months_pat + ' ' + days_pat + ord_pat + ref_pat + ', ' \
        + year_pat + end_pat
    repl = 'DMY'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    # Correct 'MMM. DD, YYYY'
    pat = start_pat + months_pat + r'\. ' + days_pat + ', ' + year_pat \
        + end_pat
    repl = 'DMY'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    # Correct 'YYYY-MM-DD'
    pat = start_pat + year_pat + r'-(?P<Month>\d{2})-(?P<Day>\d{2})' + end_pat
    repl = 'DMY'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    # Correct 'MMMMMM YYYY' with options for 'of', and ','
    pat = start_pat + months_pat + r'(?:\,)?(?: of)? ' + year_pat + end_pat
    repl = '1MY'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    # Correct 'description YYYY' with options for 'of', and ','
    pat = start_pat + r'(?P<Month>(?:W|w)inter)' + r'(?:\,)?(?: of)? ' \
        + year_pat + end_pat
    repl = '11Y'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    pat = start_pat + r'(?P<Month>(?:E|e)arly)' + r'(?:\,)?(?: of)? ' \
        + year_pat + end_pat
    repl = '12Y'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    pat = start_pat + r'(?P<Month>(?:S|s)pring)' + r'(?:\,)?(?: of)? ' \
        + year_pat + end_pat
    repl = '14Y'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    pat = start_pat + r'(?P<Month>(?:M|m)id)' + r'(?:\-| )' \
        + year_pat + end_pat
    repl = '17Y'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    pat = start_pat + r'(?P<Month>(?:S|s)ummer)' + r'(?:\,)?(?: of)? ' \
        + year_pat + end_pat
    repl = '17Y'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    pat = start_pat + r'(?P<Month>(?:L|l)ate)' + r'(?:\,)?(?: of)? ' \
        + year_pat + end_pat
    repl = '19Y'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    pat = start_pat + r'(?P<Month>(?:E|e)nd)' + r'(?:\,)?(?: of)? ' \
        + year_pat + end_pat
    repl = '19Y'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    # Correct 'YYYY'
    pat = start_pat + year_pat + end_pat
    repl = '11Y'
    df = extractDate(df, 'sf', 'dt', pat, repl)

    # Change to datetime
    df['dt'] = pd.to_datetime(df['dt'],
                              errors='coerce',
                              infer_datetime_format=True)

    return df['dt']
