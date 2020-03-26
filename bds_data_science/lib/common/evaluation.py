import numpy as np
import pandas as pd
import copy


def merge_df(left, rights, how='left') -> pd.DataFrame:
    """
    merge one DataFrame with several DataFrames
    merge on the intersection of left and rights columns
    :rtype: object
    :param how: string
    :param left: one DataFrame
    :param rights: one DataFrame or list of DataFrames
    :return: merged DataFrame
    """
    if not isinstance(rights, list):
        rights = [rights]
    for p_right in rights:
        left_cols = list(left.columns)
        right_cols = list(p_right.columns)
        on = list(set(left_cols).intersection(set(right_cols)))
        left = pd.merge(left, p_right, on=on, how=how)
    return left


def tpfn(y_true, y_pred):
    """
    fp: y_true = 0 and y_pred = 1
    fn: y_true = 1 and y_pred = 0
    tp: y_true = 1 and y_pred = 1
    tn: y_true = 0 and y_pred = 0
    """


def _clip_value(value, down_limit):
    """
    clip_value
    :param value:
    :param down_limit:
    :return:
    """
    if value < down_limit:
        value = down_limit
    else:
        pass
    return value


def weighted_mean_absolute_percent_error(y_true, y_pred):
    """
    加权平均绝对误差率
    """
    abs_error = abs(y_pred - y_true)
    sum_abs_error = np.sum(abs_error)
    sum_y = _clip_value(np.sum(y_true), 1e-6)
    return "wmape", sum_abs_error / sum_y, False


def mean_absolute_error(y_true, y_pred):
    """
    mean_absolute_error
    :param y_true:
    :param y_pred:
    :return:
    """
    abs_error = abs(y_pred - y_true)
    return "mae", np.mean(abs_error), False


def recall(y_true, y_pred):
    """
    :param y_true:
    :param y_pred:
    """


def precision(y_true, y_pred):
    pass


def f1_score(y_true, y_pred):
    pass


def count(y_true, y_pred):
    """
    count
    :param y_true:
    :param y_pred:
    :return:
    """
    return 'cnt', len(y_true), False


def metric_func_wrapper(metric_func, y_true_col, y_pred_col):
    """
    metric_func_wrapper
    :param metric_func:
    :param y_true_col:
    :param y_pred_col:
    :return:
    """

    def wrap_func(df):
        y_true = df[y_true_col]
        y_pred = df[y_pred_col]
        return metric_func(y_true, y_pred)[1]

    _y_true = np.array([1])
    _y_pred = np.array([1])
    name = metric_func(_y_true, _y_pred)[0]

    return name, wrap_func


class Evaluator:
    """
    keys = ["foo"] * 2 + ["bar"] * 2
    y = np.arange(4)
    y_pred = np.arange(4) + 1
    df1 = pd.DataFrame({'key': keys, 'actual': y, 'prediction': y_pred})
    print(df1)

    metric_funcs = [count, weighted_mean_absolute_percent_error, mean_absolute_error]
    eval_ret = Eval.cal_group_metrics((df1, 'model'), metric_funcs, 'key')
    print(eval_ret)

    df2 = df1.copy()
    df2['prediction'] = df2['prediction']+1
    print(df2)

    # 显示所有列
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    eval_ret = Eval.cal_group_metrics([(df1, 'model'), (df2, 'ema')], metric_funcs, 'key')
    print(eval_ret)

    eval_ret = Eval.cal_group_metrics([(df1, 'model'), (df2, 'ema')], metric_funcs, 'total')
    print(eval_ret)
    """
    y_true_col = 'actual'
    y_pred_col = 'prediction'

    def __init__(self):
        pass

    @classmethod
    def _cal_group_metrics_base(cls, arg, metric_func, groupby_cols):
        """计算单个预测结果的单个评估指标值，不建议外部调用
        :param arg: tuple, tuple like (pandas.DataFrame, str)
        :param metric_func: Metrics function
        :param groupby_cols: str or list of str
        :return: pandas.DataFrame
        """
        df = arg[0]
        post_fix = arg[1]
        grouped = df.groupby(groupby_cols)
        post_fix = '_' + post_fix

        name, f_eval_func = metric_func_wrapper(metric_func, cls.y_true_col, cls.y_pred_col)

        error_col_name = name + post_fix
        df_error = grouped.apply(f_eval_func).reset_index(name=error_col_name)
        return df_error

    @staticmethod
    def cal_group_metrics(args, metric_funcs, groupby_cols):
        """计算多个预测结果的多个评估指标值
        :param args: tuple or list of tuple, tuple like (pandas.DataFrame, str),
                     DataFrame columns contain 'actual' and 'prediction'
        :param metric_funcs: Metrics function or list of Metrics function
        :param groupby_cols: str or list of str
        :return: pandas.DataFrame

        """
        if not isinstance(args, list):
            args = [args]
        if not isinstance(metric_funcs, list):
            metric_funcs = [metric_funcs]
        if not isinstance(groupby_cols, list):
            groupby_cols = [groupby_cols]

        if groupby_cols == ['total']:
            args = copy.deepcopy(args)
            for arg in args:
                arg[0]['total'] = '1'

        metrics = args[0][0][groupby_cols].drop_duplicates()
        for arg in args:
            metric = [Evaluator._cal_group_metrics_base(arg, func, groupby_cols) for func in metric_funcs]
            metrics = merge_df(metrics, metric)

        if groupby_cols == ['total']:
            metrics = metrics.drop(columns='total')
        return metrics


if __name__ == '__main__':
    sp_y_true = np.array([1, 0, 1, 1, 0])
    sp_y_pred = np.array([1, 0, 0, 1, 1])
    tpfn(sp_y_true, sp_y_pred)
