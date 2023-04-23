# anaconda/python
import pandas as pd
from typing import Union
import pathlib
from utilities.tradedate import calendar
from core.simulator import Simulator
from core.strategy import Strategy
from core.streaming_data_generator import STEAMINGDATAGENERATOR
from utilities.envs import HELPDATA

def strategy_runner(contract: float, config_file: Union[pathlib.Path, str]):
    contracts = [contract]
    start_date = pd.to_datetime('2020-07-21')
    end_date = pd.to_datetime('2020-07-31')
    trade_dates = calendar.loc[start_date:end_date].index
    for date in trade_dates:
        strat = Strategy(date=date, contracts=contracts, config_file=config_file)
        sdg = STEAMINGDATAGENERATOR(date, contracts=contracts)
        simulator = Simulator()
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
                if strat.send_order_flag and not strat.stop_loss_tag:
                    strat.sendorder2simulator(simulator, 'aggressive')
                if strat.send_order_flag and strat.stop_loss_tag:
                    strat.sendorder2simulator(simulator, 'aggressive')
    return

if __name__ == "__main__":
    config_file = HELPDATA.joinpath('strategy_config.json')
    #for contract in [672009, 192009, 242009, 232009, 342009, 772009, 802009, 302009, 702012, 222009, 322009]:
    for contract in [672009]:
        strategy_runner(contract, config_file)
