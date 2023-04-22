import pathlib

PROJECT_PATH = pathlib.Path(__file__).resolve().parent.parent

# Data Path

DATA_PATH = PROJECT_PATH.joinpath('data')
CONTRACTLIST = PROJECT_PATH.joinpath('help_data').joinpath('ContractList.csv')
CALENDAR = PROJECT_PATH.joinpath('help_data').joinpath('calendar.csv')
RESULT_ROOT = PROJECT_PATH.joinpath('results')

# model paramter
HELPDATA = PROJECT_PATH.joinpath('help_data')