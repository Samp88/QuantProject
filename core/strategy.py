import pathlib
import pickle
import sys

path = pathlib.Path(__file__).parent.parent
sys.path.append(str(path))

import pandas as pd
import numpy as np
from typing import List, Dict
from core.account import Account
from utilities.constant import TickData, OrderCallback, Holding, BUYSELLDIRECTION, Order
from utilities.envs import RESULT_ROOT
from utilities.tradedate import offset_trading_day
from utilities.logger import get_logger
from core.streaming_data_generator import Streaming_data_generator
from core.simulator import Simulator

from utilities.read_data import load_data

class Strategy:
    def __init__(self, date: pd.Timestamp, contracts: List[float], log_level:str = 'INFO',**kwarg):
        self._n = 0
        self._date = date
        self.current_time: pd.Timestamp = None # local time when reciving a snapshot data
        self.current_exchtime_sec: int = 9*3600 # seconds, from exchage time, start to count at 00:00:00
        self.last_min_sec: int = 9*3600 # last time to enter a new minute.
        self.contracts = contracts
        self.min_data_list = {contract: pd.DataFrame(columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'NetInFlow']) for contract in contracts}
        self._cached_tickes = {contract: [] for contract in contracts}
        self.action_map = {}
        self.LastPrices = {}
        self.LastTicks: Dict[float, TickData] = {}
        self.pending_orders = {}
        self.log = get_logger(module_name=__name__,
                              filename='strategy',
                              user_timefunction=self.current_strftime,
                              level=log_level)

        self.__init_account('Test')

    def current_strftime(self):
        return self.current_time.strftime("%Y-%m-%d %H:%M:%S")

    def __update_time(self, data: TickData):
        time_sec = data.ExchDateTime.hour*3600 + data.ExchDateTime.minute*60 + data.ExchDateTime.second
        if time_sec < 9 * 3600  or time_sec > 15 * 3600:
            return 
        self.current_time = data.RecvDateTime
        self.current_exchtime_sec = time_sec
        return
    
    def __init_account(self, account_name: str):
        last_date = offset_trading_day(self._date, -1)
        if RESULT_ROOT.joinpath(f'{account_name}_{last_date.strftime("%Y-&m-%d")}.pickle').exists():
            with open(RESULT_ROOT.joinpath(f'{account_name}_{last_date.strftime("%Y-&m-%d")}.pickle'), "rb") as f:
                account: Account = pickle.load(f)
        else:
            Holding1 = Holding(Quantity=2, Last_Settle_Price=3700.0, FillPrice=3700.0, FillTimeStamp=pd.to_datetime('2020-07-17'), ContractId=852010.0)
            account = Account(name=account_name, init_pv= 10000000, init_holdings={Holding1.ContractId: Holding1})
        self.account = account

    def __update_lasttick(self, data: TickData):
        contractID = data.ContractId
        self.LastTicks[contractID] = data
        self.LastPrices[contractID] = data.Last
        return 
    
    def on_msg(self, msg):
        if msg is not None:
            if isinstance(msg, dict):  # 行情通用格式
                msg_type = msg['messageType']
            else:
                msg_type = type(msg).__name__
                method = getattr(self, self.action_map.get(msg_type, f"on_{msg_type}".lower()), self.on_unknown)
                return method(msg)

    def on_unknown(self, msg):
        self.log.warning(f"Received unknown msg type: {type(msg).__name__}")
        return


    def on_tickdata(self, msg: TickData):
        self._n += 1
        if self._n % 1000 == 0:
            self.log.info('Logging')
        self.__update_time(msg)
        self.__update_lasttick(msg)
        if (self.current_exchtime_sec - self.last_min_sec >= 60) and (self.last_min_sec not in [36900, 41400]):
            for ContractId in self.contracts:
                self.__update_min_data(ContractId)
            self.account.update_pv_info(self.LastPrices)
            self.log.info(self.account.assetvalue)
        self.last_min_sec = max((self.current_exchtime_sec // 60) * 60, self.last_min_sec)
        self._cached_tickes[msg.ContractId].append(msg)
        return

    def on_ordercallback(self, msg: OrderCallback):
        self.log.info(f'recieved order call back of {msg.ContractId}, \nFilled Price {msg.FillPrice}\nFilled Quantily {msg.FillQuantity}!')
        self.account.on_ordercallback(msg, self.LastPrices)
        return

    def cal_factors(self):
        raise NotImplementedError
    
    def cal_target_position(self):
        raise NotImplementedError

    def sendorder2simulator(self, target_position: Dict[float, int], target_simulator: Simulator):
        '''
        策略产生权重并发单到simulator, 同时记录已经发送但是未成交的订单。
        '''
        # TODO: 增加发送订单 send_price 的选择: 以买单为例， conservative 发 Bid， aggrisave 发 Ask， very_aggrisave 发 Ask + Ask-Bid） 
        self.log.info(f'Send Order to simulator, target position: {target_position}')
        target_order = {}
        for k, v in target_position.items():
            current_holding = self.account.holdings.get(k, None)
            if not current_holding:
                current_position = 0
            else:
                current_position = current_holding.Quantity
            quantity = v - current_position
            if quantity == 0:
                continue
            elif quantity > 0:
                Direction = BUYSELLDIRECTION.BUY
                send_price = self.LastTicks[k].Ask
            else:
                Direction = BUYSELLDIRECTION.SELL 
                send_price = self.LastTicks[k].Bid
            one_order = Order(nonce=f'test_order_{k}', 
                              TimeStamp=self.current_time, 
                              ContractId=k, 
                              SendQuantity = quantity, 
                              SendPrice = send_price, 
                              Direction = Direction)
            target_order[k] = one_order
        
        for ContractId, one_order in target_order.items():
            # send2simulator
            target_simulator.pending_order(one_order)
            self.pending_orders[ContractId] = one_order
        return

    def cancel_order(self, ContractId: float, target_simulator: Simulator):
        # take 策略, 做了简化，同一个 contract 只能挂一个单在 simulator
        cancelled_order = self.pending_orders.get(ContractId, None)
        if not cancelled_order:
            self.log.warning(f'Cancelling {ContractId} order, cannot find it in pending orders!')
        else:
            target_simulator.cancel_order(ContractId)
            self.pending_orders.pop(ContractId)
        return cancelled_order

    def __clean_cache_tick(self, ContractId: float):
        self._cached_tickes[ContractId] = []
    
    def __get_one_min_data(self, ContractId: float):
        '''
        通过收集的一分钟的tick 数据， 合成分钟数据
        '''
        ticks = self._cached_tickes[ContractId]
        if len(ticks):
            ticks = sorted(ticks, key=lambda x: x.LastVol)
            if self.last_min_sec in self.min_data_list[ContractId].index:
                unfinshed_kbar = self.min_data_list[ContractId].loc[self.last_min_sec] #incase restart the process
            else:
                unfinshed_kbar =  pd.Series(np.array((np.nan,
                                            np.nan,
                                            np.nan,
                                            np.nan,
                                            np.nan,
                                            np.nan,
                                            np.nan,
                                            np.nan)), 
                                            index=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'NetInFlow'])
            unfinshed_kbar.loc['Date'] = self.current_time.strftime('%Y-%m-%d')
            hour = (self.last_min_sec // 3600)
            minute = (self.last_min_sec%3600) // 60
            unfinshed_kbar.loc['Time'] = f'{str(int(hour)).zfill(2)}:{str(int(minute)).zfill(2)}:00'
            unfinshed_kbar.loc['Open'] = ticks[0].Last if np.isnan(unfinshed_kbar['Open']) else unfinshed_kbar['Open']
            unfinshed_kbar.loc['High'] = np.nanmax((np.nanmax([msg.Last for msg in ticks]), unfinshed_kbar['High']))
            unfinshed_kbar.loc['Low'] = np.nanmin((np.nanmin([msg.Last for msg in ticks]), unfinshed_kbar['Low']))
            unfinshed_kbar.loc['Close'] = ticks[-1].Last
            unfinshed_kbar.loc['Volume'] = np.sum([tick.LastVol for tick in ticks])
            unfinshed_kbar.loc['NetInFlow'] = 0
            return unfinshed_kbar
        else:
            return pd.Series(np.nan, index=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'NetInFlow'])
                

    def flat_all_postion(self):
        raise NotImplementedError

    def __update_min_data(self, ContractId: float):
        # TODO: implementation
        target_time = self.last_min_sec
        one_min_data = self.__get_one_min_data(ContractId)
        one_min_data = one_min_data.to_frame(target_time)
        self.min_data_list[ContractId] = pd.concat([self.min_data_list[ContractId], one_min_data.T])
        self.__clean_cache_tick(ContractId)
        return

    def exit(self, e: BaseException = None):
        """
            Strategy level error handling, including saving things, etc
        """
        self.account.settle(LastPrices=self.LastPrices)
        with open(RESULT_ROOT.joinpath(f'{self.account.AccountName}_{self._date.strftime("%Y-%m-%d")}.pickle'), "wb") as f:
            pickle.dump(self.account, f)
        mindata_savepath = RESULT_ROOT.joinpath(f'minutes/{self._date.strftime("%Y-%m-%d")}')
        if not mindata_savepath.exists():
            mindata_savepath.mkdir(parents=True)
        for ContractId in self.contracts:
            self.min_data_list[ContractId].to_csv(mindata_savepath.joinpath(f'{int(ContractId)}.csv'))
        if not e:
            self.log.debug('Finished!')
        # raise KeyboardInterrupt
    

if __name__ == '__main__':
    dates = ['2020-07-21', '2020-07-22', '2020-07-23', '2020-07-24', 
             '2020-07-27', '2020-07-28', '2020-07-29', '2020-07-30', '2020-07-31']
    for _date in dates:
        date = pd.to_datetime(_date)
        data = load_data(path.joinpath(f'data/MD_with_signal_{date.strftime("%Y%m%d")}.hdf5'))
        contracts = data['ContractId'].to_list()
        contracts = list(set(contracts))
        contracts = [x for x in contracts if x<900000]
        print('Loading Strats')
        strat = Strategy(date, contracts=contracts)
        print('Loading Data')
        sdg = Streaming_data_generator(date, contracts=contracts)
        simulator = Simulator()
        n = 0
        while True:
            msg = sdg.getNextMsg()
            if not msg:
                strat.exit()
            else:
                order_callback = simulator.on_msg(msg)
                if order_callback:
                    sdg.putMsg(order_callback, method='left')
                strat.on_msg(msg)
                n+=1
                if n%10000==0:
                    print(n)
                if n == 3000:
                    strat.sendorder2simulator({852010.0: 4}, simulator)


