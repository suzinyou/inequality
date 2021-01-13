import re
import numpy as np
import pandas as pd

from src.data.utils import load_cpi, load_sido_names, load_sigungu_names
from src.dictionary import translate
from src.env import data_dir

if __name__ == "__main__":

    mm = pd.read_excel(data_dir / '02_mean_median' / 'mean_median.xlsx', sheet_name=None)
    indices = pd.read_excel(data_dir / '04_indices' / 'indices.xlsx', sheet_name=None)

    mm_sheet_names = {k.lower(): k for k in mm.keys()}
    ind_sheet_names = {k.lower(): k for k in indices.keys()}
    new = dict()

    # for k, df in mm.items():
    #     k = k.lower()
    #     if 'gaibja_type' in k or 'smpl' in k or k.endswith('_hh') or k.endswith('_eq') or ('kr' in k and 'hh2' in k):
    #         continue
    #
    #     df = df.rename(columns={k: k.lower() for k in df.columns})
    #     df = df[[col for col in df.columns if 'rank' not in col and col != 'freq']].dropna()
    #
    #     cpi = load_cpi(translate(k.split('_')[0]))
    #
    #     df = df.merge(cpi, on='std_yyyy', how='left')
    #     df['mean_real'] = df['mean'] / df.cpi
    #     df['median_real'] = df['median'] / df.cpi
    #
    #     on_cols = ['var', 'std_yyyy']
    #     col_reorder = on_cols.copy()
    #
    #     if 'sigungu' in k:
    #         subregion_df = load_sigungu_names()
    #         df = df.merge(subregion_df, on='sigungu', how='left', ).merge(cpi, on='std_yyyy', how='left')
    #         on_cols.append('sigungu')
    #         col_reorder.extend(['sigungu', '구'])
    #     elif 'sido' in k:
    #         subregion_df = load_sido_names()
    #         df = df.merge(subregion_df, on='sido', how='left', ).merge(cpi, on='std_yyyy', how='left')
    #         on_cols.append('sido')
    #         col_reorder.extend(['sido', '시도'])
    #
    #     order_by_cols = on_cols.copy()
    #
    #     if 'earner' in k or 'adult' in k:
    #         if re.search(r'adult\d{2}_earner', k) is not None:
    #             # correct denominator (divide by number of n0+, not entire population)
    #             adultn0_k = mm_sheet_names['_'.join(k.split('_')[:-1])]
    #             adultn0_df = mm[adultn0_k].rename(columns={'STD_YYYY': 'std_yyyy'})
    #             anyvar = adultn0_df['var'].iloc[0]
    #             group_cols = on_cols.copy()
    #             group_cols.remove('var')
    #             required_cols = group_cols + ['count']
    #             ref = adultn0_df.loc[
    #                 adultn0_df['var'] == anyvar, required_cols].rename(columns={'count': 'num_indi'})
    #             df = df.drop(columns=['num_indi']).merge(ref, on=group_cols)
    #             df.loc[:, 'frac_earners'] = df['count'] / df['num_indi']
    #
    #         col_reorder.extend(['count', 'num_indi', 'frac_earners'])
    #     else:
    #         col_reorder.extend(['count'])
    #
    #     col_reorder.extend([
    #         'mean',
    #         'mean_real',
    #         'median',
    #         'median_real'])
    #
    #     if k in ind_sheet_names:
    #         on_cols = ['std_yyyy', 'var']
    #
    #         if 'sigungu' in k:
    #             on_cols.append('sigungu')
    #         elif 'sido' in k:
    #             on_cols.append('sido')
    #
    #         df_indices = indices[k]
    #         for c in df_indices.columns:
    #             if df_indices[c].dtype == float:
    #                 df_indices[c] = np.around(df_indices[c], 5)
    #         df_indices = df_indices.drop_duplicates()
    #
    #         df = df.merge(df_indices.rename(columns={k: k.lower() for k in indices[k].columns}), on=on_cols, how='left')
    #         col_reorder.extend([
    #             'gini',
    #             'iqsr',
    #             'rpr'])
    #
    #     new[k] = df[col_reorder].sort_values(by=order_by_cols).drop_duplicates()
    #
    # keys = sorted(list(new.keys()))
    #
    # with pd.ExcelWriter(data_dir / 'mean_median_and_indices.xlsx') as writer:
    #     for k in keys:
    #         df = new[k]
    #         df.to_excel(writer, index=False, sheet_name=k.lower())

    for k, df in indices.items():
        df = df.rename(columns={k: k.lower() for k in df.columns})
        k = k.lower()

        on_cols = ['var', 'std_yyyy']
        col_reorder = on_cols.copy()

        if 'sigungu' in k:
            subregion_df = load_sigungu_names()
            df = df.merge(subregion_df, on='sigungu', how='left',)
            on_cols.append('sigungu')
            col_reorder.extend(['sigungu', '구'])
        elif 'sido' in k:
            subregion_df = load_sido_names()
            df = df.merge(subregion_df, on='sido', how='left',)
            on_cols.append('sido')
            col_reorder.extend(['sido', '시도'])

        new[k] = df

    with pd.ExcelWriter(data_dir / 'indices_with_names.xlsx') as writer:
        for k, df in new.items():
            df.to_excel(writer, index=False, sheet_name=k.lower())
