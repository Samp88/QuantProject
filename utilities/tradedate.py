import os
import pathlib
import warnings
from functools import partial
from .envs import CALENDAR
import pandas as pd



def get_calendar():
    calendar = pd.read_csv(CALENDAR, index_col=0, parse_dates=True)
    return calendar


def _offset_trading_day(start_day, offset, calendar):
    assert offset != 0
    if start_day in calendar.index:
        return calendar.index[calendar.index.get_loc(start_day)+offset]
    else:
        union = list(sorted(set(calendar.index.tolist() + [start_day])))
        ix = union.index(start_day)
        if ix + offset < 0:
            raise ValueError('Exceed TRADEDATE start day')
        if ix + offset >= len(union):
            raise ValueError('Exceed TRADEDATE end day')
        return union[ix + offset]


def _has_night_session(date: pd.Timestamp, calendar) -> bool:
    assert date in calendar.index, f'<date>{date.strftime("%Y-%m-%d")} must be a trading day'
    weekday = date.weekday()
    if weekday == 0:
        last_workday = date - pd.Timedelta(days=3)
    else:
        last_workday = date - pd.Timedelta(days=1)
    last_tradeday = offset_trading_day(date, -1)
    return last_tradeday == last_workday


try:
    calendar = get_calendar()
    offset_trading_day = partial(_offset_trading_day, calendar=calendar)
    has_night_session = partial(_has_night_session, calendar=calendar)
except Exception as e:
    print(e)


def datetime2tradeday(index: pd.Index):
    '''
        用于将日内数据的 index 转换为 交易日,
        输入的index需要保证是在CTA交易时间段内 21:00 ~ 02:30, 09:00 ~ 15:00
    '''
    time = index - pd.Timedelta(hours=5)  # 使得夜盘凌晨的时间日历日在前一天
    calendar_day = pd.to_datetime(time.date)
    trade_day = [offset_trading_day(
        date, 1) if time.hour > 14 else date for date, time in zip(calendar_day, time)]
    return trade_day
