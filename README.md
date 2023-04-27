# QuantProject
A HFT Quant analysis project

To run the project, you need go to __strategy_runner.py__ and run bash command __python strategy_runner.py__.
You also need to check if you have all data needed.

```
1. snapshot data in data/
2. calendar.csv in help_data/
3. ContractList.csv in help_data/
```

You can change the model paramters by editing __help_data/strategy_config.json__ where some parameters are defined.

In order to run backtesting, you may also need some historical data, for example historical factors. I suggest to use date
'2020-07-20' to generator factor only without backtesting. To do so, you can use run_strategy.py by setting date from 2020-07-20 to 2020-07-20 and comment out cal_target_position as shown https://github.com/Samp88/QuantProject/blob/bb7ab1d272ac7f077005f382af2d554a6b3b5386/core/strategy.py#L162
