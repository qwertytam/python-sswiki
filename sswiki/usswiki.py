import pandas as pd
import re
import requests
import sys
from bs4 import BeautifulSoup

import sswiki.constants as const


def scrapeForGroupListsURLs(url):
    """Scrapes Wikipedia List of Lists article for relevant Lists articles.

    Looks for a Wikipedia "infobox" that contains list articles for vessels by
    type e.g. "List of Aircraft Carriers", "List of Battleships".

    Makes the following assumptions for where the data is to scrape:
    - In the first html class "infobox"
    - In the html table rows ("tr" elements) after the row containing "by type"

    Keyword arguments:
    url -- the url for the list of lists article to scrape

    Return:
    Pandas data frame with vessel group type (e.g. battleship) and the url to
    the lists article. Data frame will have columns with names 'group_type' and
    'url'.
    """
    COLUMNS = ['group_type', 'url']

    response = requests.get(
        url=url,
    )

    status = response.status_code

    if status != 200:
        print(f"Error with response code {status} for url {url}\n"
              + f"\n{response.headers}")
        sys.exit()

    soup = BeautifulSoup(response.content, 'html.parser')

    # Links contained in first inbox in article page
    infobox = soup.find("table", class_="infobox")

    # Will use the list of vessel by type (e.g. destroyer, battlevessel)
    # These links are in "tr" html element following the one that
    # contains "by type"
    group_hrefs = infobox.\
        find("tr", string=re.compile("by type")).\
        next_sibling.find_all("a")

    group_lists = pd.DataFrame(columns=COLUMNS)

    for group_href in group_hrefs:
        new_row = pd.DataFrame(
            [[group_href.string, const.BASE_URL + group_href['href']]],
            columns=COLUMNS)

        group_lists = pd.concat([group_lists, new_row])

    return group_lists
