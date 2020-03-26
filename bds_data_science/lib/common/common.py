import pickle5 as pickle
import pandas as pd
import zipfile
import os
from sephora_cn_recommendation_engine.lib.logger.logger_manager import LoggerManager
from logging import Logger
import functools
import operator
from datetime import date

__compression_level: dict = {
    'STORED': zipfile.ZIP_STORED,
    'DEFLATED': zipfile.ZIP_DEFLATED,
    'BZIP2': zipfile.ZIP_BZIP2,
    'LZMA': zipfile.ZIP_LZMA,
}

ctr_feature_dtype: dict = {
    'user_id': 'str',
    'op_code': 'uint32',
    'gender': 'uint8',
    'age': 'uint8',
    'city_tier': 'uint8',
    'card_type': 'uint8',
    'brand_name_en': 'uint8',
    'firstcategory': 'uint8',
    'thirdcategory': 'uint8',
    'history_purchase': 'uint8',
    'history_add': 'uint8',
    'history_click': 'uint8',
    'click_cnt': 'uint8',
    'add_cnt': 'uint8',
    'bask_cnt': 'uint8',
}

cvr_feature_dtype: dict = {
    'user_id': 'str',
    'op_code': 'uint32',
    'gender': 'uint8',
    'age': 'uint8',
    'city_tier': 'uint8',
    'card_type': 'uint8',
    'brand_name_en': 'uint8',
    'brand_group': 'uint8',
    'brandtype': 'uint8',
    'firstcategory': 'uint8',
    'secondcategory': 'uint8',
    'thirdcategory': 'uint8',
    'click_cnt': 'uint8',
    'add_cnt': 'uint8',
    'bask_cnt': 'uint8',
}

user_proflie_dtype: dict = {
    'user_id': 'str',
    'gender': 'uint8',
    'age': 'uint8',
    'city_tier': 'uint8',
    'card_type': 'uint8',
}

prod_profile_dtype: dict = {
    'op_code': 'uint32',
    'brand_name_en': 'uint8',
    'brand_group': 'uint8',
    'brand_type': 'uint8',
    'firstcategory': 'uint8',
    'secondcategory': 'uint8',
    'thirdcategory': 'uint8',
}

ueop_features_dtype: dict = {
    'user_id': 'str',
    'op_code': 'uint32',
    'history_purchase': 'uint8',
    'history_add': 'uint8',
    'history_click': 'uint8',
    'click_cnt': 'uint8',
    'add_cnt': 'uint8',
    'bask_cnt': 'uint8',
}

cvr_ueop_features_dtype: dict = {
    'user_id': 'str',
    'op_code': 'uint32',
    'click_cnt': 'uint8',
    'add_cnt': 'uint8',
    'bask_cnt': 'uint8',
}

common_logger: Logger = LoggerManager(logger_name="COMMON", log_level="INFO").logger


def deserialize_object(pickle_file_path: str):
    """
    This function is used to deserialize a python pickle file.

    :param pickle_file_path: File path of the pickle
    :return: python object else None
    """
    try:
        common_logger.info("Deserialization of the pickle file {}...".format(pickle_file_path))
        with open(pickle_file_path, "rb") as pickle_file:
            object_deserialized = pickle.load(pickle_file)
        return object_deserialized
    except Exception as e:
        common_logger.error(e)
        return None


def serialize_object(data, pickle_file_path: str, protocol: int = 4):
    """
    This function is used to serialize object as python pickle file

    :param data: Python object to serialize
    :param pickle_file_path: File path of the pickle
    :param protocol: Protocol number
    :return: True if the pickle has been created with success else False
    """
    try:
        common_logger.info("Serialization of the pickle file {}...".format(pickle_file_path))
        if isinstance(data, pd.DataFrame):
            data.to_pickle(pickle_file_path, protocol=protocol)
        else:
            with open(pickle_file_path, "wb") as pickle_file:
                pickle.dump(data, pickle_file, protocol)
        return True
    except Exception as e:
        common_logger.error(e)
        return False


def zip_file(file_to_zip, zip_file_name, arcname, compression: str = "DEFLATED"):
    """
    This function will compress data as zip file

    :param file_to_zip: File to be zipped
    :param zip_file_name: Name of the zip file
    :param arcname: This writes the file filename to the archive
    :param compression: Compression mode of the file as STORED, DEFLATED, BZIP2 and LZMA

    :return: Name of the zip file if success else None
    """
    zf = zipfile.ZipFile(zip_file_name, mode='w', compression=__compression_level[compression])
    try:
        common_logger.info("Zipping file {} to {} using compression {}...".format(file_to_zip,
                                                                                  zip_file_name,
                                                                                  compression))
        zf.write(file_to_zip, arcname)
        return file_to_zip
    except Exception as e:
        common_logger.error(e)
        return None
    finally:
        zf.close()


def unzip_file(file, directory, remove_zip_file: bool = True):
    """
    This function will uncompress zip file in a directory and remove zip file.

    :param file: Zip file
    :param directory: Directory path to uncompress
    :param remove_zip_file: True if you want to remove zip file else False
    :return: True if uncompress has succeed else False
    """
    try:
        common_logger.info("Unzipping file {} to {} ...".format(file, directory))
        with zipfile.ZipFile(file, "r") as file_to_unzip:
            file_to_unzip.extractall(directory)
        if remove_zip_file:
            os.remove(file)
        else:
            pass
        return True
    except Exception as e:
        common_logger.error(e)
        return False


def flat(list_of_list):
    """
    This function will flat a list of list

    :param list_of_list: List of list
    :return: Flatten list
    """
    return functools.reduce(operator.iconcat, list_of_list, [])


"""
Some useful functions in:
 - date operations
 - sql operations

"""


# 字符串或数字转date
def to_date_form(d):
    """
    Support string and int
    String form: YYYYMMDD or YYYY-MM-DD or YYYY.M/MM.D/DD or YYYY/MM/DD[YYYY/M/D]
    Int form: YYYYMMDD
    :param d: string or int to transform
    :return: date
    """
    if d is None:
        return None
    d = str(d)
    if '-' in d:
        d = d.split('-')
        return date(int(d[0]), int(d[1]), int(d[2]))
    if '.' in d:
        d = d.split('.')
        return date(int(d[0]), int(d[1]), int(d[2]))
    if '/' in d:
        d = d.split('/')
        return date(int(d[0]), int(d[1]), int(d[2]))
    else:
        return date(int(d[0:4]), int(d[4:6]), int(d[6:8]))


def get_last_month(yearmonth):
    """
    :param yearmonth: string/int, year+month, e.g.: 20181/201801
    :return: the yearmonth of last month
    """
    if isinstance(yearmonth, int):
        yearmonth = str(yearmonth)
    year = int(yearmonth[0:4])
    month = int(yearmonth[4:])
    if month > 1:
        ret = str(year) + str(month - 1)
    else:
        ret = str(year - 1) + '12'
    # Keep the consistence of input and output class
    if isinstance(yearmonth, int):
        return int(ret)
    else:
        return ret


def get_next_month(yearmonth):
    """
    :param yearmonth: string/int, year+month, e.g.: 20181/201801
    :return: the yearmonth of last month
    """
    if isinstance(yearmonth, int):
        yearmonth = str(yearmonth)
    year = int(yearmonth[0:4])
    month = int(yearmonth[4:])
    if month == 12:
        ret = str(year + 1) + '1'
    else:
        ret = str(year) + str(month + 1)
    # Keep the consistence of input and output class
    if isinstance(yearmonth, int):
        return int(ret)
    else:
        return ret


def list_split_g(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def list_spilt(lst, n):
    ret = []
    for i in range(0, len(lst), n):
        ret.append(lst[i: i + n])
    return ret


if __name__ == '__main__':
    x = get_last_month(201801)
    print(x)
    lst = list(range(80))
    pl = int(len(lst) / 7) + 1
    r = list_spilt(lst, pl)
    print(len(r))
    print(r)
