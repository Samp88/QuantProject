from dataclasses import dataclass
import pandas as pd
from enum import Enum

@dataclass
class TickData:
    TradingDay: float
    ContractId: float
    Bid: float
    Ask: float
    Last: float
    BidVol: float
    AskVol: float
    LastVol: float
    OpenInterest: float
    AveragePrice: float
    RecvTime: float
    ExchTime: float
    RecvDateTime: pd.Timestamp
    ExchDateTime: pd.Timestamp

class BUYSELLDIRECTION(Enum):
    BUY = 'BUY'
    SELL = 'SELL'

class ORDERSTATUS(Enum):
    FILLED = 'filled'
    CANCELED = 'cancelled'
    REJECTED = 'rejected'

@dataclass
class OrderCallback:
    nonce: str
    status: ORDERSTATUS
    TimeStamp: pd.Timestamp
    ContractId: float
    FillQuantity: int
    FillPrice: float
    Direction: BUYSELLDIRECTION
    CommissionFee: float

@dataclass
class Order:
    nonce: str
    TimeStamp: pd.Timestamp
    ContractId: float
    SendQuantity: int
    SendPrice: int
    Direction: BUYSELLDIRECTION

@dataclass
class Holding:
    ContractId: float
    Quantity: int
    Last_Settle_Price: float
    FillPrice: float
    FillTimeStamp: pd.Timestamp
