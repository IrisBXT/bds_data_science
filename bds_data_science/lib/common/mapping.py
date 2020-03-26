from typing import List
import math


def hash_mapping(seq: List[str]):
    """
    Transfer a sequence of string into a hash map
    :param seq: sequence of string
    :return: {s: abs(hash(s)}
    """
    return {s: int(math.fabs(hash(s))) for s in seq}


def index_mapping(seq: List[str]):
    """
    Transfer a sequence of string into a {str: int} map
    :param seq: sequence of string
    :return:
    """
    size = 10 ** len(str(int(len(seq))))
    return {x[1]: x[0]+size for x in enumerate(seq)}


if __name__ == '__main__':
    sp_seq = ['a123', 'b234', 'c145']
    # sp_map = hash_mapping(sp_seq)
    # print(sp_map)
    index_map = index_mapping(sp_seq)
    print(index_map)
    from sephora_cn_recommendation_engine.lib.common.io_op import save_dict, load_dict
    save_dict(index_map, 'd1.txt')
    dic_load = load_dict('d1.txt')
    print(dic_load)

