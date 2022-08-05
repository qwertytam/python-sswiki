import pandas as pd
import requests

from bs4 import BeautifulSoup
from datetime import timedelta
from ratelimit import limits, sleep_and_retry

import sswiki.utils as utils

BASE_URL = "https://en.wikipedia.org/"
STATUS_OK = 200

DATA_DIR = "../data/"

# Data frame columns for holding the links to each vessel article
VL_COLS = ["group_type",
           "group_type_url",
           "vessel_url"]

# Data items from the vessel article infobox to keep
VD_COLS = ["Acquired",
           "Active",
           "Beam",
           "Builder",
           "Builders",
           "Built",
           "Cancelled",
           "Christened",
           "Class and type",
           "2Class and type",
           "Commissioned",
           "Completed",
           "Decommissioned",
           "Displacement",
           "Draft",
           "Draught",
           "Fate",
           "Identification",
           "In commission",
           "In service",
           "Laid down",
           "Launched",
           "Length",
           "Lost",
           "Maiden voyage",
           "Name",
           "Ordered",
           "Out of service",
           "Preceded by",
           "Preserved",
           "Reclassified",
           "Recommissioned",
           "Renamed",
           "Retired",
           "Speed",
           "Status",
           "Stricken",
           "Succeeded by",
           "Tonnage",
           "Type"]


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
    print(f"Found {len(all_links)} links")

    for link in all_links:
        href = link.get('href')
        if href and pattern in href:
            vls = pd.concat([
                vls,
                pd.DataFrame(
                     [[vg['group_type'], vg['url'], BASE_URL + href]],
                     columns=VL_COLS)
            ])

    if len(vls) > 0:
        print(f"found {len(vls) - vls_len_start} vessel links "
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
    vls = pd.DataFrame(columns=VL_COLS)

    for index, row in group_lists.iterrows():
        vls = scrapeForVesselURLs(row, vls, pattern)

    vls.drop_duplicates('vessel_url', inplace=True)
    print(f"found {len(vls)} vessel links")

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
            # Data description is in the first column; check for duplicates
            # and increment where necessary
            vd.iloc[:, 0] = utils.incrementDFValues(
                vd.iloc[:, 0].astype(str))

            # Add on the vessel url and group information
            vd = pd.concat([vd, pd.DataFrame(
                [['vessel_url', vl['vessel_url']],
                 ['group_type', vl['group_type']],
                 ['group_type_url', vl['group_type_url']]],
                columns=list(vd.columns))])

            # Set the data description items as the index
            vd.set_index(
                vd.columns[0], inplace=True, verify_integrity=True)
            vd = vd.T
            vd.set_index(
                "vessel_url", inplace=True, verify_integrity=True)

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
    vd = pd.DataFrame(columns=VD_COLS)
    error_urls = []
    num_urls = len(vls)
    url_attempted = 1
    for index, vl in vls.iterrows():
        print(f"{url_attempted} of {num_urls} "
              + f"{vl['group_type']} {vl['vessel_url']}")

        new_data = scrapeVesselData(vl)
        if new_data is not None:
            vd = pd.concat([vd, new_data])
        else:
            error_urls.append(vl['vessel_url'])

        url_attempted += 1

    if len(vd) > 0 and data_csv is not None:
        vd.to_csv(DATA_DIR + data_csv)

    if len(error_urls) > 0 and error_csv is not None:
        error_urls = pd.Series(error_urls)
        error_urls.to_csv(DATA_DIR + error_csv)
    else:
        print("No error urls!")
    return vd
