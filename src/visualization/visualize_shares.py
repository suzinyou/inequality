import pandas as pd

from src.visualization.visualize import plot_shares_stacked, plot_shares_line
from src.env import data_dir


if __name__ == "__main__":
    shares = pd.read_excel(data_dir / '03_quantiles' / 'income_shares-new.xlsx', sheet_name=None, header=[0, 1])
    for k, df in shares.items():
        if 'adult20' not in k:
            continue

        df = df.drop(['Unnamed: 0_level_0'], axis=1)
        if 'STD_YYYY' in df.columns:
            df = df.rename(columns={'STD_YYYY': 'std_yyyy'})

        vnames = df[('var', 'Unnamed: 1_level_1')].unique()
        for var in vnames:
            plot_shares_stacked(df, k, var)
            # plot_shares_line(df, k, var)