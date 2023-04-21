from typing import List, Dict
import pathlib
import sys

path = pathlib.Path(__file__).parent.parent
sys.path.append(str(path))

from utilities.constant import Holding, OrderCallback
from utilities.envs import CONTRACTLIST
import pandas as pd


class Account:
    def __init__(self, name: str, init_pv: float, init_holdings: Dict[float, Holding]):
        self.AccountName = name
        self.pre_assetvalue = init_pv
        self.assetvalue = init_pv
        self.cash = init_pv
        self.unrealizedPnL: Dict[float, float] = {}
        self.holdings: Dict[float, Holding] = init_holdings
        self.ContractSize = pd.read_csv(CONTRACTLIST, index_col=0)['ContractSize']
        self.Turnover: float = 0
        self.n_trade: int = 0

    def __get_contracts(self)->List[float]:
        return self.holdings.keys()
    
    def __get_contract_holding(self, ContractID: float) -> Holding:
        one_holding = self.holdings.get(ContractID, None)
        return one_holding

    def on_ordercallback(self, msg: OrderCallback, LastPrices: Dict[float, float]):
        '''
        处理订单回调: (为了方便处理, 如果连续开仓, 选择合并持仓)
        开仓时 holdings 变化， cash 不变
        平仓时， cash = cash + realized_pnl
        asset_value = cash + unrealized_pnl
        '''
        self.cash = self.cash - msg.CommissionFee
        holding_contracts = self.__get_contracts()
        ContractId = msg.ContractId
        self.Turnover += abs(msg.FillPrice * msg.FillQuantity * self.ContractSize[ContractId])
        self.n_trade += 1
        if not ContractId in holding_contracts:
            new_holding = Holding(Quantity=msg.FillQuantity, Last_Settle_Price=msg.FillPrice,
                                  FillPrice=msg.FillPrice, FillTimeStamp=msg.TimeStamp, ContractId=ContractId)
            self.holdings[ContractId] = new_holding
        else:
            previous_holding = self.__get_contract_holding(ContractId)
            if not previous_holding:
                raise ValueError('On handle order callback, old holding can not be empty at this stage!')
            if previous_holding.Quantity * msg.FillQuantity > 0:
                # 同方向开仓
                new_position = previous_holding.Quantity+msg.FillQuantity
                new_fillprice = (previous_holding.FillPrice * previous_holding.Quantity + msg.FillPrice * msg.FillQuantity)/new_position
                new_settlement_price = (previous_holding.Last_Settle_Price * previous_holding.Quantity + msg.FillPrice * msg.FillQuantity)/new_position
                new_holding = Holding(Quantity=new_position, Last_Settle_Price=new_settlement_price,
                                  FillPrice=new_fillprice, FillTimeStamp=msg.TimeStamp, ContractId=ContractId)
                self.holdings[ContractId] = new_holding
            elif abs(previous_holding.Quantity) >= abs(msg.FillQuantity):
                # 平仓
                realized_pnl = msg.FillQuantity * (previous_holding.FillPrice - msg.FillPrice) * self.ContractSize.loc[ContractId]
                self.cash = self.cash + realized_pnl
                new_position = previous_holding.Quantity + msg.FillQuantity
                if new_position == 0:
                    self.holdings.pop(ContractId)    
                else:
                    self.holdings[ContractId].Quantity = new_position
                self.update_pv_info(LastPrices)
            else:
                # 平仓再开仓
                realized_pnl = previous_holding.Quantity * (msg.FillPrice - previous_holding.FillPrice) * self.ContractSize.loc[ContractId]
                self.cash = self.cash + realized_pnl
                new_position = previous_holding.Quantity + msg.FillQuantity
                new_fillprice = msg.FillPrice
                new_settlement_price = msg.FillPrice
                new_holding = Holding(Quantity=new_position, Last_Settle_Price=new_settlement_price,
                                  FillPrice=new_fillprice, FillTimeStamp=msg.TimeStamp, ContractId=ContractId)
                self.holdings[ContractId] = new_holding
                self.update_pv_info(LastPrices)
        return

    def get_unrealized_pnl(self, LastPrices: Dict[float, float]) -> Dict[float, float]:
        unrealized_pnl = {}
        zero_price_list = []
        for k in self.holdings.values():
            position = k.Quantity
            mark_price = k.Last_Settle_Price
            last_price = LastPrices.get(k.ContractId, 0)
            if last_price == 0:
                # 没有消息刷新价格
                last_price = mark_price
                zero_price_list.append(k)
            unrealized_pnl[k.ContractId] = unrealized_pnl.get(k.ContractId, 0) + \
                position * (last_price - mark_price) * self.ContractSize.loc[k.ContractId]
        # if len(zero_price_list):
            # self.log.warning(f'{self.AccountName} have following holdings without price!\n{zero_price_list}')
        return unrealized_pnl
    
    def update_pv_info(self, LastPrices: Dict[float, float]):
        self.unrealized_pnl = self.get_unrealized_pnl(LastPrices)
        self.assetvalue = self.cash + sum(self.unrealized_pnl.values())
        return

    def settle(self, LastPrices: Dict[float, float]):
        # Day end 结算
        self.update_pv_info(LastPrices) # 用 最后一条tick data 作结算， 更新 assetvalue, unrealized_pnl 
        zero_price_list = []
        holdings_dict = {}
        # 更新持仓的 Last_Settle_Price
        for k in self.holdings.values():
            last_price = LastPrices.get(k.ContractId, 0)
            if last_price == 0:
            # 没有消息刷新价格
                last_price = k.Last_Settle_Price
                zero_price_list.append(k)
            k.Last_Settle_Price = last_price
            holdings_dict[k.ContractId] = k
        self.holdings = holdings_dict
        # 更新 cash, unrealized_pnl
        self.cash = self.assetvalue
        self.pre_assetvalue = self.assetvalue
        self.unrealized_pnl = {}
        return

            
        



