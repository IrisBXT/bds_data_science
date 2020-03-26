import json
from functools import reduce
from typing import List, Dict
import numpy as np
import pandas as pd
from tqdm import tqdm


def column_divide(df, col_x, col_y, ret_col_name, default_nan=np.nan):
    """
    Calculate the column divide result to avoid x/0 error
    :param df: data frame
    :param col_x: column as numerator
    :param col_y: column as denominator
    :param ret: name of the result column
    :param default_nan: default value when the value of col_y is zero
    :return: col_x / col_y
    """
    df = df.copy()
    row_index = df[col_y] == 0
    df.loc[row_index, ret_col_name] = default_nan
    df.loc[-row_index, ret_col_name] = (df[col_x] / df[col_y])
    return df


def merge_df(left, rights):
    """
    merge one DataFrame with several DataFrames
    merge on the intersection of left and rights columns
    :param left: one DataFrame
    :param rights: DataFrames
    :return: merged DataFrame
    """
    for p_right in rights:
        left_cols = list(left.columns)
        right_cols = list(p_right.columns)
        on = list(set(left_cols).intersection(set(right_cols)))
        left = pd.merge(left, p_right, on=on, how='left')

    return left


def df_division(df, pieces, shuffle=False):
    """
    Divide a DataFrame into certain pieces
    :param df: DataFrame
    :param pieces: number of pieces
    :param shuffle: shuffle
    :return: list of divided DataFrames
    """
    df = df.copy()
    if shuffle:
        df = df.sample(frac=1)
    length = len(df)
    step = int(length / pieces)
    ret = []
    for i in range(0, length, step):
        ret.append(df.loc[i:i + step - 1, :])
    return ret


def df_split(df: pd.DataFrame, batch_size: int):
    ret = []
    for i in range(0, len(df), batch_size):
        ret.append(df[i: i + batch_size])
    return ret


def df_split_g(df: pd.DataFrame, batch_size: int):
    for i in range(0, len(df), batch_size):
        yield df[i: i+batch_size]


def df_sample(df, frac, shuffle=False):
    """
    Extention of df.sample, can choose not to shuffle
    :param df: Input DataFrame
    :param frac: Fraction
    :param shuffle: whether to shuffle
    :return: Part of the input DataFrame
    """
    df = df.copy()
    if shuffle:
        return df.sample(frac=frac)
    else:
        return df.loc[:len(df) * frac, :]


def to_categorical(series):
    """
    transfer the series of a DataFrame into id
    :param series: pandas.Series
    :return: a dict of value-ID mapping
    """
    if isinstance(series.dtype, pd.core.dtypes.dtypes.CategoricalDtype):
        val_set = list(series.cat.categories)
    else:
        val_set = series.copy().drop_duplicates().tolist()
    val_id_map = enumerate(val_set)
    val_id_map = {x[1]: x[0] for x in val_id_map}
    return val_id_map


def sort_ranking(df: pd.DataFrame, index_cols: list, target_col: str, top_k=10):
    s = df.groupby(index_cols)[target_col].apply(lambda x: pd.Series(
        [len(x) - k[0] for k in sorted(enumerate(np.argsort(x.values)), key=lambda i: i[1])])). \
        reset_index()[[target_col]]
    s = pd.concat([df, s.rename(columns={target_col: 'rank'})], axis=1).sort_values(by=index_cols + ['rank']). \
        query('rank <= %d' % top_k).drop(columns=['rank'])
    return s


def dict2df(dic: dict, key_col_name='key', val_col_name='val', flatten=False):
    """
    Convert a dict to data frame
    :param dic: dict to convert
    :param key_col_name: key column name
    :param val_col_name: value column name
    :param flatten: whether to flatten iterable value
        if flatten, dict like {'1': [1, 2, 3], '2': [2, 3, 4]} will be converted into a df:
              key        val
            0   1  [1, 2, 3]
            1   2  [2, 3, 4]
        else the converted df will be:
                val key
            0    1   1
            1    2   1
            2    3   1
            0    2   2
            1    3   2
            2    4   2
    :return:
    """
    if not flatten:
        return pd.DataFrame({key_col_name: list(dic.keys()), val_col_name: list(dic.values())})
    else:
        ret = []
        for k, v in dic.items():
            if len(v) == 0:
                continue
            pdf = pd.DataFrame({val_col_name: v})
            pdf[key_col_name] = k
            ret.append(pdf)
        return pd.concat(ret)


def sort_ranking_test():
    sp_df = pd.DataFrame({
        'index1': [1 for i in range(12)] + [2 for i in range(12)],
        'index2': [i for i in range(12)] + [i for i in range(12)],
        'target': [3, 4, 6, 4, 4, 7, 8, 1, 9, 12, 5, 14] + [3, 4, 6, 4, 4, 7, 8, 1, 9, 12, 5, 14]
    })
    r = sort_ranking(sp_df, ['index1'], 'target')
    print(r)


def df_flatten(df: pd.DataFrame, by: str, target: str, to_str=True):
    ret = df.groupby(by)[target].apply(lambda x: x.to_list()).reset_index()
    if to_str:
        ret[target] = ret[target].apply(lambda x: str(x)[1:-1].replace(' ', '').replace('\"', ''))
    return ret


def group_sort_select(df: pd.DataFrame, groupby, target_cols, ascending=True, top_k=10, remove_traget=False):
    groupby = [groupby] if not isinstance(groupby, list) else groupby
    target_cols = [target_cols] if not isinstance(target_cols, list) else target_cols
    index_level = len(groupby)
    index_level = 'level_%d' % index_level

    df = df.reset_index()
    ret = df.groupby(groupby)[target_cols + ['index']]. \
        apply(lambda x: x.sort_values(by=target_cols, ascending=ascending).head(top_k))
    ret = ret.reset_index().drop(columns=[index_level] + list(groupby))
    if remove_traget:
        ret=ret.drop(columns=target_cols)
    ret = pd.merge(ret, df.drop(columns=target_cols), on='index', how='left').drop(columns='index')
    return ret


def group_select(df: pd.DataFrame, groupby, nums=10):
    groupby = [groupby] if not isinstance(groupby, list) else groupby
    rest_columns = list(set(df.columns) - set(groupby))
    return df.groupby(groupby)[rest_columns].apply(
        lambda x: x.sample(n=nums) if len(x) > nums else x).reset_index().drop(columns='level_1')


def dtype_cast(df: pd.DataFrame, dtypes: Dict[str, str]):
    """

    :param df:
    :param dtypes:
    :return:
    """
    for col, dtype in dtypes.items():
        df[col] = df[col].astype(dtype)
    return df


def dict2df_test():
    sp_dict = {'1': [1, 2, 3], '2': [2, 3, 4], '4': []}
    sp_ret = dict2df(sp_dict)
    print(sp_ret)
    sp_ret1 = dict2df(sp_dict, flatten=True)
    print(sp_ret1)


def group_sort_select_test():
    sp_df = pd.DataFrame({
        'score': np.random.randint(0, 40, 1000),
        'count': np.random.randint(1, 20, 1000),
        'user_id': [x % 40 for x in range(1000)],
        'op_code': np.random.randint(1, 100, 1000)
    })
    print(sp_df)
    sp_ret = group_sort_select(sp_df, 'user_id', ['count', 'score'], ascending=False, top_k=20)
    print(sp_ret)
    print(len(sp_ret))


def df_division_test():
    sp_df = pd.DataFrame({'col1': list(range(100000))})
    # print(sp_df)

    divided = df_division(sp_df, 3)
    for x in divided:
        print(x.head(1).values)
        print(x.tail(1).values)
        print(len(x))


def df_split_test():
    df = pd.DataFrame({'col1': list(range(100000))})
    split = df_split(df, batch_size=50000)
    for df in split[1:]:
        print(len(df))
        print(df.head(1).values)
        print(df.tail(1).values)


def group_select_test():
    sp_df = pd.DataFrame({
        'score': np.random.randint(0, 40, 1000),
        'count': np.random.randint(1, 20, 1000),
        'user_id': [x % 40 for x in range(1000)],
        'op_code': np.random.randint(1, 100, 1000)
    })
    ret = group_select(sp_df, 'user_id')
    print(ret)


def df_flatten_test():
    sp_df = pd.DataFrame({
        'user_id': [x % 40 for x in range(100)],
        'op_code': list(range(100))
    })
    ret = df_flatten(sp_df, 'user_id', 'op_code')
    print(ret)


def json_de_flatten(fn, flatten_cols):
    with open(fn, 'r', encoding='utf-8') as fin:
        lst = json.load(fin)
        ret = []
        index_cols = [x for x in lst[0].keys() if x not in flatten_cols]
        flatten_size = {x: len(lst[0][x].split(',')) for x in flatten_cols}
        for dic in tqdm(lst):
            flattened = reduce(lambda x, y: {**x, **y},
                               [{"{col}_{cnt}".format(col=col, cnt=cnt): dic[col].split(',')[cnt]
                                 for col, cnt in zip(
                                       [c for _ in range(flatten_size[c])],
                                       [i for i in range(flatten_size[c])])} for c in flatten_cols])

            ret.append({
                **flattened,
                **{col: dic[col] for col in index_cols}
            })
        df = pd.DataFrame(data=ret)
        return df


def json_de_flatten_test():
    sp_fn = r'./ret.txt'
    json_de_flatten(sp_fn, ['home', 'cart', 'my'])


if __name__ == '__main__':
    # dict2df_test()
    # group_sort_select_test()
    # df_division_test()
    # df_split_test()
    # group_sort_select_test()
    # group_select_test()
    # df_flatten_test()
    json_de_flatten_test()

