import pandas as pd
import requests
import sys

from bs4 import BeautifulSoup
from datetime import timedelta
from ratelimit import limits, sleep_and_retry

import sswiki.constants as const
import sswiki.date_formatting as dfmt
import sswiki.linear_mes_formatting as lmfmt
import sswiki.weight_formatting as wfmt
import sswiki.utils as utils


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
    """

    response = requests.get(url=vl["vessel_url"])
    soup = BeautifulSoup(response.content, 'html.parser')

    # Data contained in first infobox in article page
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
        except ValueError as exc:
            msg = f"No data found for {vl['vessel_url']}\n{exc}"
            print(msg)
            vd = None

        if vd is not None:
            # Data description is in the first column; drop items we are not
            # interested in
            vd = vd[vd.iloc[:, 0].isin(const.VD_COLS)]

            # check for duplicates and increment where necessary
            vd.iloc[:, 0] = utils.incrementDFValues(
                vd.iloc[:, 0].astype(str))

            # Add on the vessel url and group information
            vd = pd.concat([vd, pd.DataFrame(
                            [['vessel_url', vl['vessel_url']],
                             ['group_type', vl['group_type']],
                             ['group_type_url', vl['group_type_url']]],
                            columns=list(vd.columns))])

            # Set the data description items as the index
            vd.set_index(vd.columns[0], inplace=True, verify_integrity=True)

            vd = vd.T
            vd.set_index("vessel_url", inplace=True, verify_integrity=True)
        return vd


def getVesselData(vls, data_csv=None, error_csv=None):
    """Scrapes Wikipedia articles for vessel information.

    Keyword arguments:
    vls -- A pandas data frame with columns for vessel group type,
        group type url, and the vessel article url
    data_csv -- path and file name string to write data to; ignored if None;
        "../data/" is pre-pended to the provided string
    error_csv -- path and file name string to store urls that returned an
        error; ignored if None; "../data/" is pre-pended to the provided string

    Return:
    A pandas data frame with vessel data for the provided article urls in vls
    """
    vd = pd.DataFrame(columns=const.VD_COLS)
    error_urls = []
    num_urls = len(vls)
    url_attempted = 1
    for index, vl in vls.iterrows():
        print(f"Scraping URL {url_attempted:>5,.0f} of {num_urls:,.0f}; "
              + f"current url is for {vl['group_type']} {vl['vessel_url']}")

        new_data = scrapeVesselData(vl)
        if new_data is not None:
            vd = pd.concat([vd, new_data])
        else:
            error_urls.append(vl['vessel_url'])

        url_attempted += 1

    if len(vd) > 0 and data_csv is not None:
        vd.to_csv(const.DATA_DIR + data_csv)

    if len(error_urls) > 0 and error_csv is not None:
        error_urls = pd.Series(error_urls)
        error_urls.to_csv(const.DATA_DIR + error_csv)
        print(f"{len(error_urls):,.0f} error urls")
    else:
        print("No error urls!")
    return vd


def convertDates(df):
    """Converts dates to a consistent format.

    Assumes that the relevant date columns are listed in the constant
    'DT_COLS'. Converts dates to datetime string default i.e. 'YYYY-MM-DD'.

    Keyword arguments:
    df -- A pandas data frame with columns for vessel data

    Return:
    A pandas data frame with vessel data and consistent date format.
    """
    # For development / testing
    df = df.sample(n=100)

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
    # For development / testing
    # df = df.sample(n=1000)

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
    # For development / testing
    # df = df.sample(n=1000)

    # regex pattern for the duplicate column name suffix - see
    # incrementDFValues()
    incr_suffix = r'(?:\_\d+)?'
    pat = (incr_suffix + "|").join(const.WT_MES)
    pat = r'(' + pat + r'(?:\_\d+)?' + r')'

    dff_cols = df.filter(regex=pat, axis=1).columns
    # print(df[dff_cols].sample(n=50))
    # df.loc[~df['Displacement'].isna(), 'Displacement'].to_csv('../tmp/tonne.csv')
    # sys.exit()
    for col in dff_cols:
        df[col] = wfmt.seriesToKnots(df[col])

        sys.exit()

    # print(df.loc[~df['Displacement'].isna(), 'Displacement'].sample(n=50))
    return df
