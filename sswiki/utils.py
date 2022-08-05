
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
