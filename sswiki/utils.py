import pandas as pd
import sswiki.constants as const


def incrementDFValues(df):
    """Increment dupicated data frame column names.

    Adds an integer increment to the end of any duplicated data frame
    column names e.g. Column, Column_2, Column_3

    Keyword arguments:
    df -- the data frame to examine for duplicated column names

    Return:
    Data frame with incremented column names as required
    """
    counter = df.groupby(df).cumcount().add(1).astype(str)
    return df.mask(df.duplicated(), df.add("_" + counter))


def loadVesselData(data_csv):
    """Load vessel data file.

    Keyword arguments:
    data_csv -- File path to file

    Return:
    A pandas data frame with vessel data from the data file.
    """
    return pd.read_csv(const.DATA_DIR + data_csv, dtype='str')


def lengthenMonth(sf):
    """Convert short month to long month.

    Keyword arguments:
    sf -- A panda series

    Return:
    A pandas series with long month names e.g. January instead of Jan.
    """
    sf = sf.str.replace(r'^Jan$', 'January', regex=True)
    sf = sf.str.replace(r'^Feb$', 'February', regex=True)
    sf = sf.str.replace(r'^Mar$', 'March', regex=True)
    sf = sf.str.replace(r'^Apr$', 'April', regex=True)
    sf = sf.str.replace(r'^Jun$', 'June', regex=True)
    sf = sf.str.replace(r'^Jul$', 'July', regex=True)
    sf = sf.str.replace(r'^Aug$', 'August', regex=True)
    sf = sf.str.replace(r'^Sep$', 'September', regex=True)
    sf = sf.str.replace(r'^Oct$', 'October', regex=True)
    sf = sf.str.replace(r'^Nov$', 'November', regex=True)
    sf = sf.str.replace(r'^Dec$', 'December', regex=True)

    return sf
