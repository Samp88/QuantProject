#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
import logging
import pandas as pd
import h5py

_logger = logging.getLogger(__name__)


def _unpack(dataset):
    colname_data = dataset.attrs["ColNames"]
    if isinstance(colname_data, str):
        col_names = colname_data.split("|")
    else:
        col_names = bytes.decode(colname_data).split("|")
    data = dataset[:]
    if data.shape[0] == len(col_names):
        data = data.T
    return pd.DataFrame(data, columns=col_names)


def load_data(file_path):
    if not os.path.isfile(file_path):
        _logger.warning(f"{file_path} does not exist!")
        return {}
    with h5py.File(file_path, "r") as f:
        _logger.debug(f"Loading {file_path}")
        groups = f.keys()
        data = {grp: _unpack(f[grp]) for grp in groups}
        return pd.concat(data.values(), axis=1)
