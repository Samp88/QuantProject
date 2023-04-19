import pathlib
import sys

path = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(path))

import pandas as pd
from typing import List, Dict
from utilities.constant import TickData, Order, OrderCallback, ORDERSTATUS,BUYSELLDIRECTION
from utilities.logger import get_logger


class Simulator:
    def __init__(self, log_level='INFO' ,**kwargs):
        self.pending_orders: Dict[float, Order] = {} # 考虑到做 take， 不会对一个 contract 同时挂买单，卖单。 简化了一下， 同一个合约只能挂一个单。 
        self.action_map = {}
        self.current_time: pd.Timestamp = pd.to_datetime('1970-01-01')
        # 1. 实例化 logger
        self.log = get_logger(module_name=__name__,
                              filename='strategy',
                              user_timefunction=self.current_strftime,
                              level=log_level)

    def current_strftime(self):
        return self.current_time.strftime("%Y-%m-%d %H:%M:%S")

    def on_msg(self, msg):
        # Order filling simulator should only handle TickData
        if not isinstance(msg, TickData):
            return
        # start check if pending order can be filled at this stage 
        if msg is not None:
            if isinstance(msg, dict):  # 行情通用格式
                msg_type = msg['messageType']
            else:
                msg_type = type(msg).__name__
                method = getattr(self, self.action_map.get(msg_type, f"on_{msg_type}".lower()), self.on_unknown)
                return method(msg)
    
    def on_tickdata(self, msg: TickData):
        '''
        '''
        ContractId = msg.ContractId
        pending_order = self.pending_orders.get(ContractId, None)
        if not pending_order:
            return
        Filled_Order = None
        if pending_order.Direction == BUYSELLDIRECTION.BUY and pending_order.SendPrice >= msg.Ask:
            Filled_Order = OrderCallback(nonce=pending_order.nonce, 
                                         status=ORDERSTATUS.FILLED, 
                                         TimeStamp=msg.ExchDateTime,
                                         ContractId = ContractId, 
                                         FillQuantity = pending_order.SendQuantity,
                                         FillPrice=msg.Ask, 
                                         Direction=pending_order.Direction, 
                                         CommissionFee=0)
            self.pending_orders.pop(ContractId)
        if pending_order.Direction == BUYSELLDIRECTION.SELL and pending_order.SendPrice <= msg.Bid:
            Filled_Order = OrderCallback(nonce=pending_order.nonce, 
                                         status=ORDERSTATUS.FILLED, 
                                         TimeStamp=msg.ExchDateTime,
                                         ContractId = ContractId, 
                                         FillQuantity = pending_order.SendQuantity,
                                         FillPrice=msg.Bid, 
                                         Direction=pending_order.Direction, 
                                         CommissionFee=0)
            self.pending_orders.pop(ContractId)
        
        return Filled_Order

    def on_unknown(self, msg):
        self.log.warning(f"Received unknown msg type: {type(msg).__name__}")
        return


    def pending_order(self, order: Order):
        ContracId = order.ContractId
        assert ContracId not in self.pending_orders, f'Can not send order of {order.ContractId} if simulator have this contract order on pending!'
        self.pending_orders[order.ContractId] = order
        return 

    
    def cancel_order(self, ContractId: float):
        assert ContractId in self.pending_orders, f'Can not cancel order of {ContractId} as it not exists!'
        self.pending_orders.pop(ContractId)
        return





