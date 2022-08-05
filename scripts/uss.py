# Hacky way to import modules instead of using setup.py or similiar
from pathlib import Path
import sys
path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)

import sswiki.sswiki as sswiki
import sswiki.usswiki as usswiki

# Script static variables
ARTICLE_PATTERN = "/wiki/USS"
LISTS_OF_LISTS_URL = sswiki.BASE_URL + "/wiki/List_of_United_States_Navy_ships"

# First find a list of vessels by type (e.g. battleship)
# The lists of lists url points to an article that has a list of the Navy ships
# by type (e.g. list of aircraft carriers, list of battleships)
group_lists = usswiki.scrapeForGroupListsURLs(LISTS_OF_LISTS_URL)


vessel_links = sswiki.getVesselLinks(group_lists, ARTICLE_PATTERN)
vessel_data = sswiki.getVesselData(vessel_links, "vessel_data.csv", "error_urls.csv")
