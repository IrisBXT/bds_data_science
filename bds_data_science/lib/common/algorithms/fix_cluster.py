import pandas as pd

from lib.connectors.sql_server import SQLServerUnit

class FixCluster:
    def __init__(self, points=pd.DataFrame, n_item: int, by=None):
        self.n_item = n_item
        self.traget = by

    def fc_distance(self, target):

