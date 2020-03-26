from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import List
from pyspark.sql import functions as f, Window


def merge_spark_dfs(left, rights, how='left_outer') -> DataFrame:
    """
    merge one DataFrame with several DataFrames
    merge on the intersection of left and rights columns
    :param left: one DataFrame
    :param rights: one DataFrame or DataFrames
    :param how: str, default 'left_outer'. One of inner, outer, left_outer, right_outer, leftsemi.
    :return: merged DataFrame

    Example:

    >>> df1 = pd.DataFrame({'key': ['A','B','C'], 'values1': [1,2,3]})
    >>> df1 = spark.createDataFrame(df1)
    >>> df2 = pd.DataFrame({'key': ['A','B'], 'values2': [3,2]})
    >>> df2 = spark.createDataFrame(df2)
    >>> merge_spark_dfs(df1,df2).show()
    +---+-------+-------+
    |key|values1|values2|
    +---+-------+-------+
    |  B|      2|      2|
    |  C|      3|   null|
    |  A|      1|      3|
    +---+-------+-------+

    """
    if not isinstance(rights, list):
        rights = [rights]
    for p_right in rights:
        left_cols = list(left.columns)
        right_cols = list(p_right.columns)
        on = list(set(left_cols).intersection(set(right_cols)))
        left = left.join(p_right, on=on, how=how)

    return left


def split(df: DataFrame, start: int or None, end: int or None) -> DataFrame:
    day_id_col = 'day_id'

    if not start and not end:
        return df
    else:
        cond1 = f.col(day_id_col) >= start
        cond2 = f.col(day_id_col) <= end

        if start and not end:
            cond = cond1
        elif not start and end:
            cond = cond2
        else:
            cond = cond1 & cond2

        return df.where(cond)


def concat(dfs: List[DataFrame]) -> DataFrame:
    """
    concat can only be performed on tables with the same number of columns
    :param dfs:
    :return:
    """
    if not dfs:
        raise ValueError("dfs must be non-empty")

    if len(dfs) == 1:
        return dfs[0]
    else:
        res = dfs[0]

        for df in dfs[1:]:
            res = res.unionAll(df)

    return res


def rename(df: DataFrame, columns: dict) -> DataFrame:
    """
    更改列名
    :param df: 输入的数据表
    :param columns: 旧列名作为键，新列名作为值，如{'old_name':'new_name'}
    :return: 更改后的表
    """
    for old_name, new_name in columns.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def cast(df: DataFrame, schemas: dict) -> DataFrame:
    """
    更改对应列的数据类型
    :param df: 输入的数据表
    :param schemas: 列名作为键，想要更改为的数据类型作为值，如{'sales':'int'};
                    数据类型包括'int','float','string','date','bool'等
    :return: 更改后的表
    """
    for col_name, d_type in schemas.items():
        df = df.withColumn(col_name, df[col_name].cast(d_type))
    return df


def get_mode_value(df, group_by_cols, value_col) -> DataFrame:
    if isinstance(group_by_cols, str):
        group_by_cols = [group_by_cols]

    cnt = df.groupBy(group_by_cols + [value_col]).agg(f.count(value_col).alias('cnt'))
    window = Window.partitionBy(group_by_cols).orderBy(f.desc('cnt'), f.desc(value_col))
    cnt = cnt.withColumn('rnk', f.row_number().over(window))
    mode_value = cnt.filter(f.col('rnk') == 1)[group_by_cols + [value_col]]
    return mode_value

