import json
import os
from datetime import datetime
import pandas as pd
import requests

from io import BytesIO, StringIO


def current_module(path):
    return os.path.split(path)[-1].split('.')[0]


def _write_line(fout, lst, sep='\t'):
    fout.write(sep.join([str(x) for x in lst]) + '\n')


def save_dict(dic: dict, fp: str):
    """
    Save a dict object to json or file
    :param dic: dict object
    :param fp: file path
    :return: None
    """
    if fp.endswith('json'):
        with open(fp, 'w', encoding='utf-8') as fout:
            json.dump(dic, fout)
    else:
        with open(fp, 'w', encoding='utf-8') as fout:
            for k, v in dic.items():
                _write_line(fout, [k, v])


def load_dict(fp: str, sep='\t', key_func=str, val_func=int):
    """
    load a dict object from file
    :param fp: file path
    :param sep: separation character
    :param key_func: dtpye function of the key
    :param val_func: dtype function of the values
    :return:
    """
    if fp.endswith('json'):
        with open(fp, 'r', encoding='utf-8') as fin:
            dic = {key_func(k): val_func(v) for k, v in json.load(fin).items()}
    else:
        dic = {}
        with open(fp, 'r', encoding='utf-8') as fin:
            for line in fin:
                k_v = line.split(sep)
                dic[key_func(k_v[0])] = val_func(k_v[1][:-1])
    return dic


def load_list(fn, func=str):
    """
    load a list from a .txt file
    :param func: cast function for element
    :param fn: load file path
    :return: list
    """
    with open(fn, encoding="utf-8") as fin:
        st = list(func(ll) for ll in fin.read().split('\n') if ll != "")
    return st


def save_list(st, ofn):
    """
    Save a list into a .txt file
    :param st: list to be saved
    :param ofn: save file path
    :return: None
    """
    with open(ofn, "w", encoding="utf-8") as fout:
        for k in st:
            fout.write(str(k) + "\n")


def load_dict_of_list(fp: str, sep='\t', key_func=str, val_func=int):
    orig_dic = load_dict(fp=fp, sep=sep, key_func=key_func, val_func=str)
    return {k: [val_func(x) for x in v[1:-1].split(', ')] if v != '[]' else [] for k, v in orig_dic.items()}


def df2json_buffer(df: pd.DataFrame, batch_size=100000):
    ret = []
    indexes = []
    for i in range(0, len(df), batch_size):
        pdf = df[i:i + batch_size]
        out_stream = StringIO()
        pdf.to_json(out_stream, orient='records')
        out_stream.flush()
        in_stream = StringIO(out_stream.getvalue())
        ret.append(in_stream)
        indexes.append(i + batch_size)
    indexes[-1] = len(df)
    return {ind: s for ind, s in zip(indexes, ret)}


def http_post(url, fp, remote_path):
    options = {'output': 'json', 'path': remote_path, 'scene': 'json', 'auth_token': "Efq2cm9ovFbrVK5k"}
    files = {'file': fp}
    r = requests.post(url, data=options, files=files)
    return r


def df2remote(df: pd.DataFrame, df_name, url, batch_size=100000):
    in_streams = df2json_buffer(df, batch_size)
    current_date = datetime.now().strftime("%Y%m%d")
    urls = []
    for ind, stream in in_streams.items():
        resp = http_post(url, stream, remote_path='{df_name}/{date}/{part}'.
                         format(df_name=df_name, date=current_date, part=ind)).json()
        urls.append(resp['url'])
    success_dic = {'total': len(df), 'url': urls}
    with open('success.json', 'w') as fout:
        json.dump(success_dic, fout)
    success_resp = http_post(url, open('success.json'), "{df_name}/{date}".format(df_name=df_name, date=current_date))
    return success_resp.json()['url']


def success_request(
        basic_url="{get_host}recosjob/v1/api/upfile?type={type}&url={url}",
        get_host="http://10.157.55.7:8088/",
        job_name=None,
        fn=None,
        success_url=None):
    """
    Request the success url
    :param get_host:
    :param fn: script_name
    :param job_name: name of the job
    :param basic_url: url body
    :param success_url: returned url of df2remote
    :return: None
    """
    if job_name is None:
        if fn is None:
            raise ValueError("Missing job_name and fn, need at least one")
        job_name = current_module(fn)[:-8]
    request_url = basic_url.format(get_host=get_host, type=job_name, url=success_url)
    requests.get(request_url)


def load_json(fn, encoding='utf-8', lines=None):
    if lines is None:
        with open(fn, 'r', encoding=encoding) as fin:
            return json.load(fin)
    else:
        # List of dicts
        ret = []
        lines = int(lines)
        c = 0
        with open(fn, 'r', encoding=encoding) as fin:
            for line in fin:
                ret.append(json.loads(line))
                c += 1
                if c == lines:
                    break
                    
        return ret


def test_df2json():
    sp_df = pd.DataFrame({'col1': list(range(300100)), 'col2': list(range(2, 300102))})
    sp_buffers = df2json_buffer(sp_df)
    for ind, b in sp_buffers.items():
        print(ind)
        print(b)


def test():
    sp_dic = {'1': 1, '2': 2}
    save_dict(sp_dic, 'demo_dict.csv')
    save_dict(sp_dic, 'demo_dict_json.json')

    sp_load_dic = load_dict('demo_dict.csv', key_func=str, val_func=str)
    print(sp_load_dic)
    sp_load_dic_json = load_dict('demo_dict_json.json')
    print(sp_load_dic_json)


def dic_lst_test():
    sp_fp = r'D:\work\sephora_cn_recommendation_engine\sephora_cn_recommendation_engine\data\process\ubcf_track_true.csv'
    for k, v in load_dict_of_list(sp_fp).items():
        print(k)
        print(v)


def http_post_test():
    url = "http://139.217.221.171:8081/group1/upload"
    fn = "success.json"
    options = {'output': 'json', 'path': 'upload', 'scene': 'json'}
    with open(fn, 'rb') as fin:
        files = {'file': fin}
        r = requests.post(url, data=options, files=files)
        print(r.text)


def df2remote_test():
    sp_df = pd.DataFrame({'col1': list(range(1, 300101)), 'col2': list(range(2, 300102))})
    url = "http://139.217.221.171:8081/group1/upload"
    df2remote(sp_df, 'upload_test_retry_3', url)


if __name__ == '__main__':
    # test()
    # dic_lst_test()
    # test_df2json()
    # http_post_test()
    # df2remote_test()
    pass
