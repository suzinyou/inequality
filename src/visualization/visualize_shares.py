import pandas as pd

from src.visualization.visualize import plot_shares_stacked, plot_shares_line
from src.env import data_dir


if __name__ == "__main__":
    shares = pd.read_excel(data_dir / '03_quantiles' / 'income_shares.xlsx', sheet_name=None)
    for k, df in shares.items():
        if 'STD_YYYY' in df.columns:
            df = df.rename(columns={'STD_YYYY': 'std_yyyy'})

        vnames = df['var'].unique()
        for var in vnames:
            plot_shares_stacked(df, k, var)
            plot_shares_line(df, k, var)