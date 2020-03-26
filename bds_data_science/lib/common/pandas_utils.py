"""
pandas utils, some functions and tools to process pandas.DataFrame and pandas.Series
"""

import pandas as pd


def df_cast(df: pd.DataFrame, dtype_dict: dict):
    """
    Cast the data type of a DataFrame

    :param df: DataFrame to cast
    :param dtype_dict: {column: data type}
    :return: df
    """
    for col, dtype in dtype_dict.items():
        if dtype == 'datetime':
            df[col] = pd.to_datetime(df[col])
        else:
            try:
                df[col] = df[col].astype(dtype)
            except ValueError:
                raise ValueError('Unable to convert column %s to %s' % (col, dtype))

    return df


def df_col_rename(df: pd.DataFrame, colname_dict: dict):
    """
    Rename the columns of a DataFrame
    :param df: DataFrame to be renamed
    :param colname_dict: {column: column name}
    :return: DataFrame renamed
    """
    df=df.rename(columns=colname_dict)
    return df

