import os
import numpy as np
import pandas as pd
from openpyxl import load_workbook

import plotly.io as pio
from src.visualization.visualize import plot_lines, save_lorenz_curve
from src.data.utils import load_cpi
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


def get_income_share_summary(df_centile, k):
    """
    :param df_centile: pd.DataFrame
        preprocessed {region}_{unit}_centile.csv !! (rank is 1~100)
    :param k: str
        key
    """
    centile_range = {
        '하위 20%': (0, 20),
        '다음 30%': (20, 50),
        '하위 50%': (0, 50),
        '중위 30%': (50, 80),
        '상위 20%': (80, 100),
        '상위 10%': (90, 100),
        '상위 1%': (99, 100),
    }
    results = list()

    groupcols = ['std_yyyy', 'var']
    yearly_count = df_centile.groupby(['var', 'std_yyyy']).max()['year_count'].rename('max_freq')

    cpi = load_cpi(translate(k.split('_')[0]))
    freq_adjustments = {}

    # how many centiles(?) are just 0?
    zero_thresh_mask = df_centile['rank'].diff() > 1.0
    zero_thresh = df_centile.loc[zero_thresh_mask]

    # add 0 ranks
    expanded_centiles = []
    for (year, var), gdf in df_centile.groupby(groupcols):
        zero_fillers = {'std_yyyy': [], 'var': [], 'rank': [], 'freq': [], 'rank_sum': [], 'share': []}
        mask = (zero_thresh['std_yyyy'] == year) & (zero_thresh['var'] == var)
        if mask.sum() == 0:
            expanded_centiles.append(gdf)
            continue
        t = int(zero_thresh[mask].iloc[0]['rank'])
        year_total = yearly_count[(var, year)]
        for i in range(2, t):
            zero_fillers['std_yyyy'].append(year)
            zero_fillers['var'].append(var)
            zero_fillers['rank'].append(i)
            zero_fillers['freq'].append(int(np.around(year_total / 100)))
            zero_fillers['rank_sum'].append(0)
            zero_fillers['share'].append(0)

        gdf.loc[gdf['rank'] == 1, 'freq'] = year_total * ((t-1) / 100) - np.sum(zero_fillers['freq'])
        gdf.loc[gdf['rank'] == t, 'freq'] = year_total - gdf.loc[(gdf['rank'] < t) | (gdf['rank'] > t), 'freq'].sum() - np.sum(zero_fillers['freq'])
        expanded = pd.concat([gdf, pd.DataFrame(zero_fillers)]).sort_values(by=groupcols + ['rank'])
        expanded_centiles.append(expanded)

    expanded_centiles = pd.concat(expanded_centiles)

    for name, r in centile_range.items():
        mask = (expanded_centiles['rank'] > r[0]) & (expanded_centiles['rank'] <= r[1])

        if mask.sum() == 0:
            # Find max_freq: expected number of people in this income group
            # (number of ppl can be very different because during quantile ranking,
            #   in case of a tie the individual was assigned the lower rank)
            max_freq = ((r[1] - r[0]) * yearly_count / 100).apply(lambda x: int(np.around(x)))

            _df = yearly_count.reset_index().drop(columns=['year_count'])
            _df = _df.merge(max_freq.rename('freq').reset_index())
            _df['share'] = 0
            _df['group_mean'] = 0
            _df['group_mean_real'] = 0
        else:
            _df = expanded_centiles[mask].copy()

            _df = _df.groupby(groupcols).agg({'rank_sum': 'sum', 'freq': 'sum', 'share': 'sum'}).reset_index()
            _df = _df.merge(cpi, on='std_yyyy', how='left')
            _df['group_mean'] = _df['rank_sum'] / _df['freq']
            _df['group_mean_real'] = _df['group_mean'] / _df.cpi
            _df = _df.drop(columns=['rank_sum'])

        _df.loc[:, 'income_group'] = name
        results.append(_df)

    df = pd.concat(results, axis=0).sort_values(by=['std_yyyy', 'var'])
    df = df.pivot(index=['var', 'std_yyyy'], columns=['income_group'], values=['freq', 'group_mean', 'group_mean_real', 'share']).reset_index()
    sorted_groups = ['하위 20%', '다음 30%', '하위 50%', '중위 30%', '상위 20%', '상위 10%', '상위 1%']
    df = df[[('var', ''), ('std_yyyy', '')] +
            [('freq', k) for k in sorted_groups] +
            [('group_mean', k) for k in sorted_groups] +
            [('group_mean_real', k) for k in sorted_groups] +
            [('share', k) for k in sorted_groups]]
    return df


if __name__ == "__main__":

    qts = pd.read_excel(data_dir / '03_quantiles' / 'quantiles.xlsx', sheet_name=None)
    qts_processed = dict()
    shares = dict()
    for k, df in qts.items():
        if 'SMPL' in k or ('kr' in k and 'hh2' in k):
            continue

        if 'STD_YYYY' in df.columns:
            df = df.rename(columns={'STD_YYYY': 'std_yyyy'})
        df_processed = preprocess_quantiles(df)
        qts_processed[k] = df_processed

        res = get_income_share_summary(df_processed, k)
        shares[k] = res

    with pd.ExcelWriter(data_dir / '03_quantiles' / 'quantiles_with_cumshares.xlsx') as writer:
        for k, df in qts_processed.items():
            df.to_excel(writer, index=False, sheet_name=k.lower())

    writer = pd.ExcelWriter(data_dir / '03_quantiles' / 'income_shares-new.xlsx', engine="xlsxwriter")
    workbook = writer.book
    format1 = workbook.add_format({'num_format': '#,##0'})
    format2 = workbook.add_format({'num_format': '0.000'})
    for k, df in shares.items():
        df.to_excel(writer, index=True, sheet_name=k.lower())

        worksheet = writer.sheets[k.lower()]
        worksheet.set_column('D:X', 12, format1)
        worksheet.set_column('Y:AE', 12, format2)
    writer.save()
    del writer

        # split = k.split('_')
        # region = split[0]
        # unit = '_'.join(split[1:])
        #
        # res.to_csv(data_dir / '03_quantiles' / f"{region}_{unit}_income_share.csv", index=False)
        #
        # # SAVE PLOTS (make argparse argument switch!)
        # # Lorenz curve
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
        #     'Middle 30%',
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
