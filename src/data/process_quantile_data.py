import os
import numpy as np
import pandas as pd


import plotly.io as pio
from src.visualization.visualize import plot_lines, save_lorenz_curve
from src.env import project_dir, data_dir
from src.dictionary import translate, region2ko, unit2ko


def preprocess_quantiles(_df):
    """Preprocess files of type `{region}_{unit}_centile.csv` and `{region}_{unit}_top1p_1000tile.csv`"""
    df = _df.copy()

    # drop null rank (for variables for which null values were excluded from analysis)
    df = df[df['rank'].notnull()]

    # ranks are named 0~99 -> change to 1~100
    df['rank'] = df['rank'] + 1

    # compute total income/property tax base for each rank group
    # df.loc[:, 'rank_sum'] = df['rank_mean'] * df['freq']

    # compute and merge yearly total income/property tax base to eventually compute quantile shares
    yearly_tot = df.groupby(['std_yyyy', 'var']).agg({'rank_sum': 'sum', 'freq': 'sum'}).reset_index().rename(
        columns={'rank_sum': 'year_sum', 'freq': 'year_count'})
    df = df.merge(yearly_tot, on=['std_yyyy', 'var'])

    df.loc[:, 'share'] = df['rank_sum'] / df['year_sum']

    # compute cumulative share to plot Lorenz Curve
    df.loc[:, 'cumshare'] = np.nan
    for g, gdf in df.groupby(['std_yyyy', 'var']):
        year, vname = g
        cs = gdf['share'].cumsum()
        df.loc[(df['std_yyyy'] == year) & (df['var'] == vname), 'cumshare'] = cs

    return df


# def approx_gini(_df):
#     """compute approximate Gini coefficient using quantile data"""
#     gini_data = {'std_yyyy': [], 'var': [], 'gini': [], 'iqsr': []}
#     for (year, var), _df in _df.groupby(['std_yyyy', 'var']):
#         gini_data['std_yyyy'].append(year)
#         gini_data['var'].append(var)
#
#         # simple gini
#         gini_approx = (0.5 - np.sum(_df.cumshare * _df['freq'] / _df['year_count'])) / 0.5
#         gini_data['gini'].append(gini_approx)
#
#         # also compute income quintile share ratio???
#         bottom20_share = _df.share[_df.rank <= 20].sum()
#         top20_share = _df.share[_df.rank > 80].sum()
#         iqsr = top20_share / bottom20_share
#         gini_data['iqsr'] = iqsr
#
#     gini_df = pd.DataFrame(gini_data)  # has columns std_yyyy, var, gini, iqsr
#     return gini_df


def get_income_share_summary(df_centile):
    """
    :param df_centile: pd.DataFrame
        preprocessed {region}_{unit}_centile.csv !! (rank is 1~100)
    :param df_toppct: pd.DataFrame
        preprocessed {region}_{unit}_top1p_1000tile.csv !!
    """
    masks = {
        'Bottom 20%': df_centile['rank'] < 20,
        'Next 30%': (df_centile['rank'] >= 30) & (df_centile['rank'] < 50),
        'Bottom 50%': df_centile['rank'] < 50,
        'Middle 40%': (df_centile['rank'] >= 50) & (df_centile['rank'] < 90),
        'Top 10%': df_centile['rank'] >= 90,
        'Top 1%': df_centile['rank'] == 100,
    }
    results = list()

    groupcols = ['std_yyyy', 'var']
    cols = groupcols + ['share']

    for name, m in masks.items():
        shares = df_centile.loc[m, cols].groupby(groupcols).sum().reset_index()

        shares.loc[:, 'income_group'] = name
        results.append(shares)

    return pd.concat(results, axis=0)[['var', 'std_yyyy', 'income_group', 'share']].sort_values(by=['std_yyyy', 'var'])


if __name__ == "__main__":

    qts = pd.read_excel(data_dir / '03_quantiles' / 'quantiles.xlsx', sheet_name=None)
    qts_processed = dict()
    shares = dict()
    for k, df in qts.items():
        if 'STD_YYYY' in df.columns:
            df = df.rename(columns={'STD_YYYY': 'std_yyyy'})
        df_processed = preprocess_quantiles(df)
        qts_processed[k] = df_processed

        res = get_income_share_summary(df_processed)
        shares[k] = res

    with pd.ExcelWriter(data_dir / '03_quantiles' / 'quantiles_with_cumshares.xlsx') as writer:
        for k, df in qts_processed.items():
            df.to_excel(writer, index=False, sheet_name=k.lower())

    with pd.ExcelWriter(data_dir / '03_quantiles' / 'income_shares.xlsx') as writer:
        for k, df in shares.items():
            df.to_excel(writer, index=False, sheet_name=k.lower())

            # res.to_csv(data_dir / '03_quantiles' / f"{region}_{unit}_income_share.csv", index=False)

            # SAVE PLOTS (make argparse argument switch!)
            # Lorenz curve
            # for y in [2006, 2010, 2014, 2018]:
            #     for v in ['inc_wage', 'inc_tot', 'prop_txbs_hs']:
            #         save_lorenz_curve(centiles, unit, y, v)
            #
            # # Gini trend
            # plot_lines(gini_df,
            #            y='gini',
            #            title=f"Approximated Gini coefficient ({translate(region)} {translate(unit)})",
            #            save_name=f"lorenz_curve_approx-{region}_{unit}.png")
            #
            # # Income share trend
            # income_groups = [
            #     'Bottom 20%',
            #     'Next 30%',
            #     'Bottom 50%',
            #     'Middle 40%',
            #     'Top 10%',
            #     'Top 1%',
            #     'Top 0.1%']
            #
            # for g in income_groups:
            #     _df = res[res['income_group'] == g]
            #     plot_lines(_df,
            #                y="share",
            #                title=g,
            #                save_name=f"income_share-{region}_{unit}-{g[:-1]}.png")
