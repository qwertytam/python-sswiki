from script_imports import utils, const, sswiki

# For GitBash on Windows 10
# sys.stdin.reconfigure(encoding='utf-8')
# sys.stdout.reconfigure(encoding='utf-8')

# Script static variables
ARTICLE_PATTERN = "wiki/USS_"
LISTS_OF_LISTS_URL = const.BASE_URL + "/wiki/List_of_United_States_Navy_ships"

FN_GC_DATA = 'gc_data.csv'
FN_SH_DATA = 'sh_data.csv'
FC_ERRORS = 'errors.csv'

# First find a list of vessels by type (e.g. battleship)
# The lists of lists url points to an article that has a list of the Navy ships
# by type (e.g. list of aircraft carriers, list of battleships)
group_lists = sswiki.scrapeForGroupListsURLs(LISTS_OF_LISTS_URL)

# Now find the links to each vessel article,
# then scrape the data from each article
vessel_links = sswiki.getVesselLinks(group_lists, ARTICLE_PATTERN)
gc, sh = sswiki.getVesselData(vessel_links, FN_GC_DATA, FN_SH_DATA, FC_ERRORS)

# Format general characteristics
gc = utils.loadVesselData(const.DATA_DIR + FN_GC_DATA, index_col='uuid')
gc = utils.dfStrNormalize(gc)
gc = sswiki.convertLinearMeasures(gc, const.LNMES_GC_COLS)
gc = sswiki.convertWeightMeasures(gc, const.WTMES_GC_COLS)
gc = sswiki.convertSpeedMeasures(gc, const.SPMES_GC_COLS)
gc.to_csv(const.DATA_DIR + 'gc_data.csv')
print("Finished general characteristics\n")

# Format service history
sh = utils.loadVesselData(const.DATA_DIR + FN_SH_DATA, index_col='uuid')
sh = utils.dfStrNormalize(sh)
sh = sswiki.convertDates(sh, const.DT_SH_COLS)
sh = sswiki.convertHullNo(sh)
sh = sswiki.getFates(sh)
sh.to_csv(const.DATA_DIR + 'sh_data.csv')
print("Finished service history\n")
