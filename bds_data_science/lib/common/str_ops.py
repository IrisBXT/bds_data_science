"""
Some useful functions for string process and NLP

Some of the functions are borrowed from LJQ, please refer to
https://github.com/Lsdefine/attention-is-all-you-need-keras/blob/master/ljqpy.py

"""

import re
import numpy as np


def is_chs_str(z):
    return re.search('^[\u4e00-\u9fa5]+$', z) is not None


def lcs(s1, s2):
    """
    Longest Child Substring of two strings
    :param s1: string 1
    :param s2: string 2
    :return: the lcs and length of the lcs
    """
    matrix = np.zeros(shape = [len(s1) + 1, len(s2) + 1])
    cursor = 0
    mmax = 0
    for i in range(len(s1)):
        for j in range(len(s2)):
            if s1[i] == s2[j]:
                matrix[i+1][j+1] = matrix[i][j]+1
                if matrix[i+1][j+1] > mmax:
                    mmax = matrix[i+1][j+1]
                    cursor = i+1
    mmax = int(mmax)
    return s1[cursor-mmax:cursor], mmax


def strQ2B(ustring):
    """
    全角半角转换
    :param ustring: input string
    :return: output string
    """
    ret = ''
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288:
            inside_code = 32
        elif 65281 <= inside_code <= 65374:
            inside_code -= 65248
        ret += chr(inside_code)
    return ret


# 只保留中文
def chs_filter(orig_str, reserved=None):
    """

    :param orig_str:
    :return:
    """
    if reserved is None:
        reserved = ['T恤', 'T-恤', 't恤', 't-恤']
    reserve_flag = False
    res_pat = ''
    for res_word in reserved:
        if re.findall(res_word, orig_str):
            orig_str = re.sub(res_word, '安全临时替换天王盖地虎', orig_str)
            res_pat = res_word
            reserve_flag = True
            break
    other_noise = ['·', '·']
    ret_str = strQ2B(orig_str)
    ret = [x for x in ret_str if not (ord(x) >= 33 and ord(x) <= 126 and x not in other_noise)]
    ret_str = ''.join(ret)
    if reserve_flag:
        ret_str = re.sub('安全临时替换天王盖地虎', res_pat, ret_str)
    return ret_str


def remove_noise(orig_str):
    """
    去除符号
    remove some common noise of a string
    :param orig_str: input string
    :return: clean string
    """
    noise_range = [x for x in range(33, 127) if
                   (x not in range(48, 58) and x not in range(97, 123) and x not in range(65, 91))]
    return ''.join([x for x in orig_str if ord(x) not in noise_range])


# list转SQL可用字符串
def list_2_sql_list(orig_list):
    """
    Transform a list to a string that SQL could use
    list -> string, to solve cases when length of list is 1
    [x1,x2,...xk] -> (x1,x2,...,xk)
    :param orig_list: original list
    :return: string list
    """
    if orig_list is None:
        return None
    return '(' + ','.join([str(x) for x in orig_list]) + ')'


if __name__ == '__main__':
    pass


