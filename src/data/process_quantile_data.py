import os
import numpy as np
import pandas as pd


import plotly.io as pio
from src.visualization.visualize import plot_lines, save_lorenz_curve
from src.env import project_dir, data_dir
from src.dictionary import translate, region2ko, unit2ko


def preprocess_quantiles(_df, _df_toppct):
    """Preprocess files of type `{region}_{unit}_centile.csv` and `{region}_{unit}_top1p_1000tile.csv`"""

    df = _df.copy()

    # drop null rank (for variables for which null values were excluded from analysis)
    df = df[df['rank'].notnull()]

    # ranks are named 0~99 -> change to 1~100
    df['rank'] = df['rank'] + 1

    # compute total income/property tax base for each rank group
    df.loc[:, 'rank_sum'] = df['rank_mean'] * df['freq']

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

    # do similar preprocessing for the top 1%'s 1000-tile data (i.e. top 1% split into deciles)
    dftp = _df_toppct.copy()
    dftp['rank'] = dftp['rank'] + 1
    dftp = dftp.merge(yearly_tot, on=['std_yyyy', 'var'])

    # remember bottom 99%'s share so we can compute cumulative share for the top 1%
    bottom99 = df.loc[(df['rank'] == 99), ['std_yyyy', 'var', 'cumshare']].rename(columns={'cumshare': 'bottom99'})
    dftp = dftp.merge(bottom99, on=['std_yyyy', 'var'])

    # compute quantile share and cumulative share
    dftp.loc[:, 'share'] = dftp['rank_sum'] / dftp['year_sum']
    dftp.loc[:, 'cumshare'] = np.nan
    for g, gdf in dftp.groupby(['std_yyyy', 'var']):
        year, vname = g
        cs = gdf['share'].cumsum() + gdf['bottom99']
        dftp.loc[(dftp['std_yyyy'] == year) & (dftp['var'] == vname), 'cumshare'] = cs

    return df, dftp


def approx_gini(_df):
    """compute approximate Gini coefficient using quantile data"""
    gini_data = {'std_yyyy': [], 'var': [], 'gini': [], 'iqsr': []}
    for (year, var), _df in _df.groupby(['std_yyyy', 'var']):
        gini_data['std_yyyy'].append(year)
        gini_data['var'].append(var)

        # simple gini
        gini_approx = (0.5 - np.sum(_df.cumshare * _df['freq'] / _df['year_count'])) / 0.5
        gini_data['gini'].append(gini_approx)

        # also compute income quintile share ratio???
        bottom20_share = _df.share[_df.rank <= 20].sum()
        top20_share = _df.share[_df.rank > 80].sum()
        iqsr = top20_share / bottom20_share
        gini_data['iqsr'] = iqsr

    gini_df = pd.DataFrame(gini_data)  # has columns std_yyyy, var, gini, iqsr
    return gini_df


def get_income_share_summary(df_centile, df_toppct):
    masks = {
        'Bottom 20%': df_centile['rank'] < 20,
        'Next 30%': (df_centile['rank'] >= 30) & (df_centile['rank'] < 50),
        'Bottom 50%': df_centile['rank'] < 50,
        'Middle 40%': (df_centile['rank'] >= 50) & (df_centile['rank'] < 90),
        'Top 10%': df_centile['rank'] >= 90,
        'Top 1%': df_centile['rank'] == 100,
        'Top 0.1%': df_toppct['rank'] == 10,
    }
    results = list()

    groupcols = ['std_yyyy', 'var']
    cols = groupcols + ['share']

    for name, m in masks.items():
        if name == 'Top 0.1%':
            shares = df_toppct.loc[m, cols].groupby(groupcols).sum().reset_index()
        else:
            shares = df_centile.loc[m, cols].groupby(groupcols).sum().reset_index()

        shares.loc[:, 'income_group'] = name
        results.append(shares)

    return pd.concat(results, axis=0)[['var', 'std_yyyy', 'income_group', 'share']].sort_values(by=['std_yyyy', 'var'])


if __name__ == "__main__":

    for region in region2ko:
        for unit in unit2ko:
            centile_fname = data_dir / '03_quantiles' / f'{region}_{unit}_centile.csv'
            toppct_fname = data_dir / '03_quantiles' / f'{region}_{unit}_top1p_1000tile.csv'
            if not os.path.exists(centile_fname) or os.path.exists(toppct_fname):
                print(f"File not found. Skipping: {centile_fname} and {toppct_fname}")
                continue

            centiles = pd.read_csv(centile_fname)
            toppct = pd.read_csv(toppct_fname)

            centiles, toppct = preprocess_quantiles(centiles, toppct)

            centiles.to_csv(data_dir / '03_quantiles' / f'{region}_{unit}_centile+share.csv')
            toppct.to_csv(data_dir / '03_quantiles' / f'{region}_{unit}_top1p_1000tile+share.csv')

            gini_df = approx_gini(centiles)
            gini_df.to_csv(data_dir / '03_quantiles' / f'{region}_{unit}_indices.csv')

            res = get_income_share_summary(centiles, toppct)
            res.to_csv(data_dir / '03_quantiles' / f"{region}_{unit}_income_share.csv", index=False)

            # SAVE PLOTS (make argparse argument switch!)
            # Lorenz curve
            for y in [2006, 2010, 2014, 2018]:
                for v in ['inc_wage', 'inc_tot', 'prop_txbs_hs']:
                    save_lorenz_curve(centiles, unit, y, v)

            # Gini trend
            plot_lines(gini_df,
                       y='gini',
                       title=f"Approximated Gini coefficient ({translate(region)} {translate(unit)})",
                       save_name=f"lorenz_curve_approx-{region}_{unit}.png")

            # Income share trend
            income_groups = [
                'Bottom 20%',
                'Next 30%',
                'Bottom 50%',
                'Middle 40%',
                'Top 10%',
                'Top 1%',
                'Top 0.1%']

            for g in income_groups:
                _df = res[res['income_group'] == g]
                plot_lines(_df,
                           y="share",
                           title=g,
                           save_name=f"income_share-{region}_{unit}-{g[:-1]}.png")

            