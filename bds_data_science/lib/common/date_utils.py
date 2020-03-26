import datetime


def get_date_by_gap(end_date: str, day_gap: int):
    """
    This function is going to figure out the start date of data time period

    :param day_gap:
    :param end_date:
    :param gap:
    :return: start_date: str
    """
    start_date = datetime.datetime.strftime(
        datetime.datetime.strptime(end_date, '%Y-%m-%d') + datetime.timedelta(days=-day_gap + 1), '%Y-%m-%d')
    return start_date


def get_date_array(end_date: str, gap_array=None):
    """

    :param end_date:
    :param gap_array:
    :return:
    """
    if gap_array is None:
        gap_array = [3, 7, 14, 30, 365]
    date_dict = {}
    for day_gap in gap_array:
        date_dict[day_gap] = get_date_by_gap(end_date, day_gap)
    return date_dict
