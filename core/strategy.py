import pathlib
import pickle
import sys
from typing import Dict, List

path = pathlib.Path(__file__).parent.parent
sys.path.append(str(path))

import numpy as np
import pandas as pd

from core.account import Account
from core.simulator import Simulator
from core.streaming_data_generator import STEAMINGDATAGENERATOR
from utilities.constant import (BUYSELLDIRECTION, Holding, Order,
                                OrderCallback, TickData)
from utilities.envs import RESULT_ROOT
from utilities.logger import get_logger
from utilities.read_data import load_data
from utilities.tradedate import offset_trading_day


class Strategy:
    def __init__(self, date: pd.Timestamp, contracts: List[float], log_level: str = 'INFO', **kwarg):
        self._n = 0
        self._date = date
        # local time when reciving a snapshot data
        self.current_time: pd.Timestamp = None
        # seconds, from exchage time, start to count at 00:00:00
        self.current_exchtime_sec: int = 9*3600
        self.last_min_sec: int = 9*3600  # last time to enter a new minute.
        self._last_sig_sec: int = 9*3600
        self.contracts = contracts
        self.min_data_list = {contract: pd.DataFrame(
            columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'ABV_Diff','ZT']) for contract in contracts}
        self._cached_tickes = {contract: [] for contract in contracts}
        self.action_map = {}
        self.LastPrices = {}
        self.LastTicks: Dict[float, TickData] = {}
        self.pending_orders = {}
        self.factors: Dict[float, pd.DataFrame] = {}
        self.send_order_flag = False
        self.last_target_position: Dict[float, int] = {}
        self.log = get_logger(module_name=__name__,
                              filename=f'strategy_{self.contracts[0]}',
                              user_timefunction=self.current_strftime,
                              level=log_level)
        self.__init_account('Test')

    def current_strftime(self):
        return self.current_time.strftime("%Y-%m-%d %H:%M:%S")

    def __update_time(self, data: TickData):
        time_sec = data.ExchDateTime.hour*3600 + \
            data.ExchDateTime.minute*60 + data.ExchDateTime.second
        if time_sec < 9 * 3600 or time_sec > 15 * 3600:
            return
        self.current_time = data.ExchDateTime
        self.current_exchtime_sec = time_sec
        return

    def __init_account(self, account_name: str):
        last_date = offset_trading_day(self._date, -1)
        if RESULT_ROOT.joinpath(f'{account_name}_{last_date.strftime("%Y-%m-%d")}.pickle').exists():
            with open(RESULT_ROOT.joinpath(f'{account_name}_{last_date.strftime("%Y-%m-%d")}.pickle'), "rb") as f:
                account: Account = pickle.load(f)
        else:
            account = Account(name=account_name,
                              init_pv=10000000, init_holdings={})
        self.account = account

    def __update_lasttick(self, data: TickData):
        contractID = data.ContractId
        self.LastTicks[contractID] = data
        self.LastPrices[contractID] = data.Last
        return

    def on_msg(self, msg):
        self.send_order_flag = False
        if msg is not None:
            if isinstance(msg, dict):  # 行情通用格式
                msg_type = msg['messageType']
            else:
                msg_type = type(msg).__name__
                method = getattr(self, self.action_map.get(
                    msg_type, f"on_{msg_type}".lower()), self.on_unknown)
                return method(msg)

    def on_unknown(self, msg):
        self.log.warning(f"Received unknown msg type: {type(msg).__name__}")
        return

    def on_tickdata(self, msg: TickData):
        self._n += 1
        if self._n % 10000 == 0:
            self.log.info('Logging')
        self.__update_time(msg)
        self.__update_lasttick(msg)
        if (self.current_exchtime_sec - self.last_min_sec >= 60) and (self.last_min_sec not in [36900, 41400]):
            for ContractId in self.contracts:
                self.__update_min_data(ContractId)
            self.account.update_pv_info(self.LastPrices)
            self.log.info(self.account.assetvalue)
            self.__cal_factors()
        
        if self.current_exchtime_sec - self._last_sig_sec >= 10:
           self.cal_target_position()
        
        self.last_min_sec = max(
            (self.current_exchtime_sec // 60) * 60, self.last_min_sec)
        self._cached_tickes[msg.ContractId].append(msg)
        return

    def on_ordercallback(self, msg: OrderCallback):
        self.log.info(
            f'recieved order call back of {msg.ContractId}, Filled Price {msg.FillPrice} Filled Quantily {msg.FillQuantity}!')
        self.log.info(self.account.cash)
        self.pending_orders.pop(msg.ContractId)
        self.account.on_ordercallback(msg, self.LastPrices)
        self.log.info(self.account.cash)
        return

    def __cal_factors(self):
        for ContractId in self.contracts:
            min_data = self.min_data_list[ContractId]
            min_close = min_data['Close']
            factor_MOMT_5M = ((min_close-min_close.shift(5))/min_close.shift(5)).to_frame('Momt5')
            factor_rwR = ((min_data['Close'] - min_data['Open'].shift(4))/(min_data['High'].rolling(5).max() - min_data['Low'].rolling(5).min())).to_frame('rwR5')
            self.factors[ContractId] = pd.concat([factor_MOMT_5M, factor_rwR], axis=1)

    def cal_target_position(self):
        self.log.debug('In signal caculation!')
        self._last_sig_sec = (self.current_exchtime_sec) // 10 * 10
        target_position = {}
        for ContractId in self.contracts:
            current_holding = self.account.holdings.get(ContractId, None)
            if self.current_exchtime_sec > 14*3600+55*60 or self.current_exchtime_sec < 9*3600+5*60: 
                if current_holding:
                    target_position[ContractId] = 0
                    self.send_order_flag = True
                else:
                    return
            signal = np.sign(self.factors[ContractId].fillna(
                0)).iloc[-1]['rwR5']
            if not current_holding:
                if signal != 0:
                    target_position[ContractId] = - signal * round(self.account.assetvalue / len(
                        self.contracts) / self.account.ContractSize.loc[ContractId] / self.LastTicks[ContractId].Last)
                    self.send_order_flag = True
            else:
                Filled_time = current_holding.FillTimeStamp
                Filled_time_sec = Filled_time.hour * 3600 + \
                    Filled_time.minute*60 + Filled_time.second
                Filled_Price = current_holding.FillPrice
                if self.current_exchtime_sec - Filled_time_sec > 295:
                    target_position[ContractId] = 0
                    self.send_order_flag = True
                
                # if np.sign(current_holding.Quantity)*(self.LastTicks[ContractId].Last - Filled_Price)/Filled_Price > 0.002:
                #    self.log.info('Send stop profit task')
                #    target_position[ContractId] = 0
                #    self.send_order_flag = True
                
                #if np.sign(current_holding.Quantity)*(self.LastTicks[ContractId].Last - Filled_Price)/Filled_Price < -0.001:
                #    self.log.info('Send stop loss task')
                #    target_position[ContractId] = 0
                #    self.send_order_flag = True

        self.last_target_position = target_position
        return

    def sendorder2simulator(self, target_simulator: Simulator):
        '''
        策略产生权重并发单到simulator, 同时记录已经发送但是未成交的订单。
        '''
        target_position = self.last_target_position
        # TODO: 增加发送订单 send_price 的选择: 以买单为例， conservative 发 Bid， aggrisave 发 Ask， very_aggrisave 发 Ask + Ask-Bid）
        self.log.info(
            f'Send Order to simulator, target position: {target_position}')
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
                send_price = self.LastTicks[k].Last
            else:
                Direction = BUYSELLDIRECTION.SELL
                send_price = self.LastTicks[k].Last
            one_order = Order(nonce=f'test_order_{k}',
                              TimeStamp=self.current_time,
                              ContractId=k,
                              SendQuantity=quantity,
                              SendPrice=send_price,
                              Direction=Direction)
            target_order[k] = one_order

        for ContractId, one_order in target_order.items():
            # send2simulator
            pending_orders = self.pending_orders.get(ContractId, None)
            if pending_orders:
                cancelled_order = self.cancel_order(ContractId, target_simulator)
                if not cancelled_order:
                    return
            target_simulator.pending_order(one_order)
            self.pending_orders[ContractId] = one_order
        return

    def cancel_order(self, ContractId: float, target_simulator: Simulator):
        # take 策略, 做了简化，同一个 contract 只能挂一个单在 simulator
        cancelled_order = self.pending_orders.get(ContractId, None)
        if not cancelled_order:
            self.log.warning(
                f'Cancelling {ContractId} order, cannot find it in pending orders!')
        else:
            order = target_simulator.cancel_order(ContractId)
            if not order:
                self.log.warning('cannot cancel order, as it is finished!')
                return
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
                # incase restart the process
                unfinshed_kbar = self.min_data_list[ContractId].loc[self.last_min_sec]
            else:
                unfinshed_kbar = pd.Series(np.nan, index=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'ABV_Diff', 'ZT'])
            unfinshed_kbar.loc['Date'] = self.current_time.strftime('%Y-%m-%d')
            hour = (self.last_min_sec // 3600)
            minute = (self.last_min_sec % 3600) // 60
            unfinshed_kbar.loc['Time'] = f'{str(int(hour)).zfill(2)}:{str(int(minute)).zfill(2)}:00'
            unfinshed_kbar.loc['Open'] = ticks[0].Last if np.isnan(
                unfinshed_kbar['Open']) else unfinshed_kbar['Open']
            unfinshed_kbar.loc['High'] = np.nanmax(
                (np.nanmax([msg.Last for msg in ticks]), unfinshed_kbar['High']))
            unfinshed_kbar.loc['Low'] = np.nanmin(
                (np.nanmin([msg.Last for msg in ticks]), unfinshed_kbar['Low']))
            unfinshed_kbar.loc['Close'] = ticks[-1].Last
            unfinshed_kbar.loc['Volume'] = np.sum(
                [tick.LastVol for tick in ticks])
            unfinshed_kbar.loc['Amount'] = np.sum(
                [tick.LastVol * tick.AveragePrice * self.account.ContractSize.loc[ContractId] for tick in ticks])
            
            # netinflow
            # v = np.array([tick.LastVol for tick in ticks])
            a = np.array([tick.Ask for tick in ticks])
            b = np.array([tick.Bid for tick in ticks])
            # avg = np.array([tick.AveragePrice for tick in ticks])
            # avg = np.clip(avg, a_min=b, a_max=a) # 考虑到 tick 之间的价格变化 以及 成交价可能在bid2, ask2 以上
            # x: ratio of fill price at a1. x*a1 + (1-x)*b1 = avg_price
            # x = (avg - b) / (a - b)
            # netinflow_volume = x * v - (1-x) * v
            # unfinshed_kbar.loc['Netinflow_Volume'] = np.round(np.nansum(netinflow_volume), 4)
            
            # 挂单量差值的1min积累 买单挂单量-卖单挂单量
            av = np.array([tick.AskVol for tick in ticks])
            bv = np.array([tick.BidVol for tick in ticks])
            unfinshed_kbar.loc['ABV_Diff'] = np.nansum(av-bv)

            # 指令单薄与指令单流
            ZT = np.log([tick.Last for tick in ticks]) - np.log((b + a) / 2)
            unfinshed_kbar.loc['ZT'] = np.mean(ZT)

            return unfinshed_kbar
        else:
            return pd.Series(np.nan, index=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'ABV_Diff', 'ZT'])

    def flat_all_position(self):
        raise NotImplementedError

    def __update_min_data(self, ContractId: float):
        # TODO: implementation
        target_time = self.last_min_sec
        one_min_data = self.__get_one_min_data(ContractId)
        one_min_data = one_min_data.to_frame(target_time)
        self.min_data_list[ContractId] = pd.concat(
            [self.min_data_list[ContractId], one_min_data.T])
        self.__clean_cache_tick(ContractId)
        return

    def exit(self, e: BaseException = None):
        """
            Strategy level error handling, including saving things, etc
        """
        self.account.settle(LastPrices=self.LastPrices)
        with open(RESULT_ROOT.joinpath(f'{self.account.AccountName}_{self._date.strftime("%Y-%m-%d")}.pickle'), "wb") as f:
            pickle.dump(self.account, f)
        mindata_savepath = RESULT_ROOT.joinpath(
            f'minutes/{self._date.strftime("%Y-%m-%d")}')
        if not mindata_savepath.exists():
            mindata_savepath.mkdir(parents=True)
        for ContractId in self.contracts:
            self.min_data_list[ContractId].to_csv(
                mindata_savepath.joinpath(f'{int(ContractId)}.csv'))
            
        factor_savepath = RESULT_ROOT.joinpath(
            f'factors/{self._date.strftime("%Y-%m-%d")}')
        if not factor_savepath.exists():
            factor_savepath.mkdir(parents=True)
        for ContractId in self.contracts:
            self.factors[ContractId].to_csv(
                factor_savepath.joinpath(f'{int(ContractId)}.csv'))
        if not e:
            self.log.debug('Finished!')
        return
        # raise KeyboardInterrupt


if __name__ == '__main__':
    dates = ['2020-07-20', '2020-07-21', '2020-07-22', '2020-07-23', '2020-07-24', 
            '2020-07-27', '2020-07-28', '2020-07-29', '2020-07-30', '2020-07-31']
    for _date in dates:
        date = pd.to_datetime(_date)
        data = load_data(path.joinpath(
            f'data/MD_with_signal_{date.strftime("%Y%m%d")}.hdf5'))
        # contracts = [322009, 672009, 262010, 142009, 702012, 242009, 842009, 362009, 802009, 332009, 152012, 872010, 
        #             682009, 772009, 792009, 382009, 762011, 302009, 372009, 352009, 882009, 652009, 222009, 502009, 
        #             342009, 232009, 422009, 452009, 212010, 462009, 312009, 192009]
        contracts = [192009]
        print('Loading Strats')
        strat = Strategy(date, contracts=contracts)
        print('Loading Data')
        sdg = STEAMINGDATAGENERATOR(date, contracts=contracts)
        simulator = Simulator()
        n = 0
        while True:
            msg = sdg.getNextMsg()
            if not msg:
                strat.exit()
                break
            else:
                order_callback = simulator.on_msg(msg)
                if order_callback:
                    sdg.putMsg(order_callback, method='left')
                strat.on_msg(msg)
                if strat.send_order_flag:
                    strat.sendorder2simulator(simulator)
