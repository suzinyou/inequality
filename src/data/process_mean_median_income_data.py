import os
import numpy as np
import pandas as pd


import plotly.io as pio
from src.visualization.visualize import plot_lines, save_lorenz_curve
from src.env import project_dir, data_dir, map_dir
from src.dictionary import translate, region2ko, unit2ko


def load_cpi():
    """Load consumer price index data"""
    cpi = pd.read_excel(data_dir / 'external' / '소비자물가지수_서울과 전국__20201113113326.xlsx',
                        index_col='시도별(2)').T.iloc[2:]
    cpi /= 100
    cpi = cpi.reset_index().rename(
        columns={'index': 'std_yyyy', '소계': '전국', '서울특별시': '서울'}
    ).astype(
        {'std_yyyy': int, '전국': float, '서울': float}
    )
    return cpi


def load_sido_names():
    sidodf = pd.read_csv(map_dir / 'CTPRVN.csv')


def load_sigungu_name():
    sigungudf = pd.read_csv(map_dir / 'SIG.csv')
    seoul_mask = sigungudf['SIG_CD'].apply(lambda x: str(x).startswith('11'))
    sigungudf = sigungudf[seoul_mask]
    sigungudf['SIG_CD'].apply(lambda x: str(x)[2:])
    return sigungudf