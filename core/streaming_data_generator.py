import pathlib
import sys

path = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(path))

import datetime
from collections import deque
import pandas as pd
from typing import List
from dacite import from_dict
from utilities.constant import TickData
from utilities.read_data import load_data
from utilities.envs import DATA_PATH

def _timestamp2datetime(timestamp: int) -> pd.Timestamp:
    assert timestamp > 0, 'Timestamp should great than 0'
    dt =  datetime.datetime(1, 1, 1) + datetime.timedelta(seconds=timestamp/10000000)
    return pd.Timestamp(dt)

class Streaming_data_generator:
    def __init__(self, date: pd.Timestamp, contracts: List[float]):
        self._date = date
        self.q = deque()
        self.contracts = contracts
        self.__generate_data_queue()
    

    def __generate_data_queue(self):
        _df_tick_data: pd.DataFrame = load_data(DATA_PATH.joinpath(f'MD_with_signal_{self._date.strftime("%Y%m%d")}.hdf5'))
        _df_tick_data = _df_tick_data.sort_values(by='RecvTime')

        for idx in _df_tick_data.index:
            if _df_tick_data.loc[idx, "ContractId"] in self.contracts:
                one_tick = _df_tick_data.loc[idx].to_dict()
                one_tick['RecvDateTime'] = _timestamp2datetime(one_tick['RecvTime'])
                one_tick['ExchDateTime'] = _timestamp2datetime(one_tick['ExchTime'])
                if one_tick['ExchDateTime'].hour < 9 or one_tick['ExchDateTime'].hour > 15:
                    continue
                self.q.append(from_dict(TickData, one_tick))
    
    def getNextMsg(self):
        if self.q:
            return self.q.popleft()
        else:
            return None
        
    def putMsg(self, msg, method:str):
        # put new msg into the data deque
        if method == 'left':
            self.q.appendleft(msg)
        elif method=='right':
            self.q.append(msg)
        else:
            raise ValueError('put message method can only be left or right')

if __name__ == "__main__":
    sdg = Streaming_data_generator(pd.to_datetime('2020-07-20'), contracts=[852010.0])
    print('Done!')
