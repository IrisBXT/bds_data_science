import os


class Config:
    def __init__(self, env='dev'):
        """
        Project configuration
        :param env:
        """
        if env == 'dev':
            self.root_dir = "/data/app/data/"
            self.ftp_host = "http://139.217.222.175:8080/"
            self.get_host = "http://139.217.222.175:9000/"

        elif env == 'master':
            self.root_dir = "/mnt/resource/data/"
            self.ftp_host = "http://10.157.55.7:8080/"
            self.get_host = "http://10.157.55.7:8088/"

        elif env == 'test':
            self.root_dir = "D:/test/"
            self.ftp_host = None
            self.get_host = None

        else:
            raise KeyError("env %s not supported" % env)


if __name__ == '__main__':
    conf = Config()
    data_path = 'ubcf/'
    data_path = conf.root_dir + data_path
    print(data_path)

