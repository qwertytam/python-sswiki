import pandas as pd
import requests
import uuid

from bs4 import BeautifulSoup
from datetime import timedelta
from ratelimit import limits, sleep_and_retry

import sswiki.constants as const
import sswiki.date_formatting as dfmt
import sswiki.hull_no_formatting as hnfmt
import sswiki.linear_mes_formatting as lmfmt
import sswiki.speed_formatting as spfmt
import sswiki.weight_formatting as wfmt
import sswiki.utils as utils

import sys


def scrapeForVesselURLs(vg, vls, pattern):
    """Scrapes Wikipedia lists article for relevant Naval vessel article links.

    Looks for a Wikipedia article urls that contains the given pattern; if the
    pattern is found, then add the url to vl.

    Keyword arguments:
    vg -- a data frame with the list article information to scan
        for vessel articles; data frame should contain the columns "group_type"
        and "url"
    vl -- a data frame to add the vessel article links to
    pattern -- a string pattern to find in the desired vessel article link e.g.
        "wiki/USS" for United States Navy Ships

    Return:
    A pandas data frame with columns for vessel group type, group type url, and
    the vessel article url
    """
    print(f"Processing {vg['url']}")
    vls_len_start = len(vls)

    response = requests.get(url=vg['url'])
    soup = BeautifulSoup(response.content, 'html.parser')
    all_links = soup.find_all("a")
    print(f"Found {len(all_links):,.0f} links")

    for link in all_links:
        href = link.get('href')
        if href and pattern in href:
            vls = pd.concat([
                vls,
                pd.DataFrame(
                     [[vg['group_type'], vg['url'], const.BASE_URL + href]],
                     columns=const.VL_COLS)
            ])

    if len(vls) > 0:
        print(f"Found {len(vls) - vls_len_start:,.0f} vessel links "
              + f"for {vg['group_type']}")

    return vls


def getVesselLinks(group_lists, pattern):
    """Get Naval vessel article links.

    Looks for a Wikipedia article urls that contains the given pattern; if the
    pattern is found, then add the url to vl.

    Keyword arguments:
    group_lists -- a data frame with the list article information to scan
        for vessel articles; data frame should contain the columns "group_type"
        and "url"
    pattern -- a string pattern to find in the desired vessel article link e.g.
        "wiki/USS" for United States Navy Ships

    Return:
    A pandas data frame with columns for vessel group type, group type url, and
    the vessel article url
    """
    vls = pd.DataFrame(columns=const.VL_COLS)

    for index, row in group_lists.iterrows():
        vls = scrapeForVesselURLs(row, vls, pattern)

    vls.drop_duplicates('vessel_url', inplace=True)
    print(f"Found {len(vls):,.0f} vessel links")

    return vls


@ sleep_and_retry
@ limits(calls=1, period=timedelta(microseconds=250).total_seconds())
def scrapeVesselData(vl):
    """Scrapes Wikipedia article for vessel information.

    Keyword arguments:
    vl -- A one row pandas data frame with columns for vessel group type,
        group type url, and the vessel article url

    Return:
    A pandas data frame with vessel data for the provided article url in vl
    with columns 'desc' and 'data'. Will return `None` if infoxbox not found or
    unexpected shape (less than two columns).
    """

    response = requests.get(url=vl["vessel_url"])
    soup = BeautifulSoup(response.content, 'html.parser')

    infobox = soup.find("table", class_="infobox")

    if infobox is None:
        vd = None
    else:
        try:
            # Assume vessel data is in the first two columns of the
            # first table found by read_html
            vd = pd.read_html(str(infobox))[0].iloc[:, 0:2]
            if len(vd.columns) < 2:
                vd = None
            else:
                vd.columns = ['desc', 'data']
                vd.fillna(value="N/A", inplace=True)

        except ValueError as e:
            msg = f"No data found for {vl['vessel_url']}\n{e}\nReturning None"
            print(msg)
            vd = None

    return vd


def getVesselGenCharacteristics(vd):
    """Gets 'general characteristics' of vessel from provided vessel data.

    Looks for the pattern r'^general characteristics' (case insentive) in the
    'desc' column; then splits the DataFrame, with all rows after the
    matching row returned and dropped from 'vd' (in place).

    Keyword arguments:
    vd -- A two column pandas data frame with columns 'desc' for description
        and 'data' for data. 'vd' is modified with matching returned dropped.

    Return:
    A pandas data frame with general characterisic vessel data.
    """
    pat = r'^general characteristics'
    try:
        gc_row = vd.index[vd['desc'].str.match(pat, case=False)].tolist()[0]
    except IndexError as e:
        gc = None
    else:
        gc = vd.iloc[gc_row + 1:, :]
        vd.drop(vd.index[gc_row:], inplace=True)

    return gc


def getVesselServiceHistory(vd):
    """Gets 'service history' of vessel from provided vessel data.

    Looks for the pattern r'^name$' or r'^history$' (case insentive) in the
    'desc' column. Splits 'vd' at each occurence and returns it.

    Keyword arguments:
    vd -- A two column pandas data frame with columns 'desc' for description
        and 'data' for data.

    Return:
    A list of pandas data frames, with each data frame vessel service history.
    """
    pat = r'^name$'
    idx = vd.index[vd['desc'].str.match(pat, case=False)].tolist()
    idx = [max(0, x - 1) for x in idx] + [vd.shape[0]]
    # print(f"\nidx:{idx}\n")
    if len(idx) == 1:
        # See if "History" is found
        # print("Using r'history")
        pat = r'^history$'
        idx = vd.index[vd['desc'].str.match(pat, case=False)].tolist()
        idx = [x + 1 for x in idx] + [vd.shape[0]]

    if len(idx) == 1:
        idx.insert(0, 1)  # for case not captured above

    # list of data frames split at all indices
    sh = [vd.iloc[idx[n]:idx[n + 1]] for n in range(len(idx) - 1)]

    return sh


def cleanVesselData(vd, vl, country=None, keep_cols=const.VD_COLS):
    """Cleans provided data frame and appends relevant information.

    Keeps columns as provided, appends vessel link and country information,
    then transposes and adds a 'uuid' for unique identification.

    Keyword arguments:
    vd -- A two column pandas data frame with columns 'desc' for description
        and 'data' for data.
    vl -- A three column pandas data frame with columns 'vessel_url',
        'group_type', and 'group_type_url'
    country -- String country name to include in return.
    keep_cols -- List of string column names to keep in `vd`.

    Return:
    The cleaned data frame 'vd'.
    """
    # Data description is in the first column; drop items we are not
    # interested in
    vd = vd[vd.iloc[:, 0].isin(keep_cols)].copy()

    # check for duplicates and increment where necessary
    vd.iloc[:, 0] = utils.incrementDFValues(vd.iloc[:, 0].astype(str))

    # Add on the vessel url and group information
    vd = pd.concat([vd, pd.DataFrame(
                    [['vessel_url', vl['vessel_url']],
                     ['group_type', vl['group_type']],
                     ['group_type_url', vl['group_type_url']],
                     ['uuid', uuid.uuid4().hex]],
                    columns=list(vd.columns))])

    if country:
        vd = pd.concat([vd, pd.DataFrame(
                            [['country', country]],
                            columns=list(vd.columns))])

    # Set the data description items as the index
    vd.set_index(vd.columns[0], inplace=True, verify_integrity=True)

    vd = vd.T
    vd.set_index('uuid', inplace=True, verify_integrity=True)
    return vd


def getVesselData(vls, gcdata_csv=None, shdata_csv=None, error_csv=None):
    """Scrapes Wikipedia articles for vessel information.

    Keyword arguments:
    vls -- A pandas data frame with columns for vessel group type,
        group type url, and the vessel article url
    gcdata_csv -- path and file name string to write vessel general
        characterisic data to; ignored if None; "../data/" is pre-pended to
        the provided string
    shdata_csv -- path and file name string to write vessel service
        history data to; ignored if None; "../data/" is pre-pended to
        the provided string
    error_csv -- path and file name string to store urls that returned an
        error; ignored if None; "../data/" is pre-pended to the provided string

    Return:
    A tuple of two pandas data frame (gc, sh); gc for vessel general
        characterisics and sh for vessel service history
    """
    gc = pd.DataFrame(columns=const.VD_COLS)
    sh = pd.DataFrame(columns=const.VD_COLS)
    error_urls = []
    num_urls = len(vls)
    url_no = 1
    print_int = 50

    for index, vl in vls.iterrows():
        if url_no % print_int == 0 or url_no == 1 or url_no == num_urls:
            print(f"Scraping URL {url_no:>5,.0f} of {num_urls:,.0f}; "
                  + f"current url is for {vl['group_type']} "
                  + f"{vl['vessel_url']}")

        new_data = scrapeVesselData(vl)
        if new_data is not None:
            gc_new = getVesselGenCharacteristics(new_data)
            if gc_new is not None:
                gc_new = cleanVesselData(gc_new, vl)
                gc = pd.concat([gc, gc_new])
            else:
                error_urls.append(vl['vessel_url'])

            sh_new = getVesselServiceHistory(new_data)
            for shn in sh_new:
                shn = cleanVesselData(shn, vl, shn.iloc[0, 0])
                sh = pd.concat([sh, shn])

        else:
            error_urls.append(vl['vessel_url'])

        url_no += 1

    if len(gc) > 0 and gcdata_csv is not None:
        gc.to_csv(const.DATA_DIR + gcdata_csv, index_label='uuid')

    if len(sh) > 0 and shdata_csv is not None:
        sh.to_csv(const.DATA_DIR + shdata_csv, index_label='uuid')

    if len(error_urls) > 0 and error_csv is not None:
        error_urls = pd.Series(error_urls)
        error_urls.to_csv(const.DATA_DIR + error_csv, index=False)
        print(f"{len(error_urls):,.0f} error urls")
    else:
        print("No error urls!")

    return gc, sh


def convertDates(df):
    """Converts dates to a consistent format.

    Assumes that the relevant date columns are listed in the constant
    'DT_COLS'. Converts dates to datetime string default i.e. 'YYYY-MM-DD'.

    Keyword arguments:
    df -- A pandas data frame with columns for vessel data

    Return:
    A pandas data frame with vessel data and consistent date format.
    """

    # regex pattern for the duplicate column name suffix - see
    # incrementDFValues()
    incr_suffix = r'(?:\_\d+)?'
    pat = (incr_suffix + "|").join(const.DT_COLS)
    pat = r'(' + pat + r'(?:\_\d+)?' + r')'

    dff_cols = df.filter(regex=pat, axis=1).columns
    for col in dff_cols:
        df[col] = dfmt.seriesToDateTime(df[col])

    date_columns = df.select_dtypes(include=['datetime64']).columns.tolist()
    df[date_columns] = df[date_columns].astype(str)

    return df


def convertLinearMeasures(df):
    """Converts linear meeasurements (length, beam, draft) to a consistent
    format.

    Assumes that the relevant columns are listed in the constant
    'LN_MES'. Converts all measures to metres and columns to numeric.

    Keyword arguments:
    df -- A pandas data frame with columns for vessel data

    Return:
    A pandas data frame with vessel data and consistent measurement format.
    """

    # regex pattern for the duplicate column name suffix - see
    # incrementDFValues()
    incr_suffix = r'(?:\_\d+)?'
    pat = (incr_suffix + "|").join(const.LN_MES)
    pat = r'(' + pat + r'(?:\_\d+)?' + r')'

    dff_cols = df.filter(regex=pat, axis=1).columns
    for col in dff_cols:
        df[col] = lmfmt.seriesToMetres(df[col])

    return df


def convertWeightMeasures(df):
    """Converts weight meeasurements (displacement, tonnage) to a consistent
    format.

    Assumes that the relevant columns are listed in the constant
    'WT_MES'. Converts all measures to metric tons and columns to numeric. For
    surface ships uses standard displacement if given; for submarines surface
    displacement.

    Keyword arguments:
    df -- A pandas data frame with columns for vessel data

    Return:
    A pandas data frame with vessel data and consistent measurement format.
    """

    # regex pattern for the duplicate column name suffix - see
    # incrementDFValues()
    incr_suffix = r'(?:\_\d+)?'
    pat = (incr_suffix + "|").join(const.WT_MES)
    pat = r'(' + pat + r'(?:\_\d+)?' + r')'

    dff_cols = df.filter(regex=pat, axis=1).columns
    for col in dff_cols:
        df[col] = wfmt.seriesToTonnes(df[col])

    return df


def convertSpeedMeasures(df):
    """Converts speed meeasurements (knots) to a consistent
    format.

    Keyword arguments:
    df -- A pandas data frame with columns for vessel data

    Return:
    A pandas data frame with vessel data and consistent measurement format.
    """

    # regex pattern for the duplicate column name suffix - see
    # incrementDFValues()
    incr_suffix = r'(?:\_\d+)?'
    pat = (incr_suffix + "|").join(const.SP_MES)
    pat = r'(' + pat + r'(?:\_\d+)?' + r')'

    dff_cols = df.filter(regex=pat, axis=1).columns
    for col in dff_cols:
        df[col] = spfmt.seriesToKnots(df[col])

    return df


def convertHullNo(df):
    """Converts speed meeasurements (knots) to a consistent
    format.

    Keyword arguments:
    df -- A pandas data frame with columns for vessel data

    Return:
    A pandas data frame with vessel data and consistent measurement format.
    """
    df.columns.values[0] = 'vessel_url'
    df = hnfmt.seriesToHullNo(df, 'vessel_url')

    return df
