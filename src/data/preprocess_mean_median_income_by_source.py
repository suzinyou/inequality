import numpy as np
import pandas as pd

from src.data.utils import load_cpi, load_sido_names, load_sigungu_names
from src.dictionary import translate
from src.env import data_dir

if __name__ == "__main__":

    mm = pd.read_excel(data_dir / '02_mean_median' / 'mean_median-by_source.xlsx', sheet_name=None)

    mm_sheet_names = {k.lower(): k for k in mm.keys()}
    new = dict()

    for k, df in mm.items():
        df = df.drop_duplicates()
        df = df.rename(columns={k: k.lower() for k in df.columns})
        k = k.lower()

        cpi = load_cpi(translate(k.split('_')[0]))

        df = df.merge(cpi, on='std_yyyy', how='left')

        df['소득자비율(%)'] = np.round(df['frac_earner'] * 100, decimals=1)
        for col in df.columns:
            if 'mean' in col or 'median' in col:
                df[translate(col) + '(천원)'] = df[col] / 1000
        for col in df.columns:
            if 'mean' in col or 'median' in col:
                split = col.split('_')
                stat, var = split[0], '_'.join(split[1:])
                df[translate(f'{stat}_real_' + var) + '(천원)'] = df[col] / df.cpi / 1000

        new[k] = df

    with pd.ExcelWriter(data_dir / 'mean_median-by_source_processed.xlsx') as writer:
        for k, df in new.items():
            df.to_excel(writer, index=False, sheet_name=k.lower())

    # for k, df in indices.items():
    #     df = df.rename(columns={k: k.lower() for k in df.columns})
    #     k = k.lower()
    #
    #     on_cols = ['var', 'std_yyyy']
    #     col_reorder = on_cols.copy()
    #
    #     if 'sigungu' in k:
    #         subregion_df = load_sigungu_names()
    #         df = df.merge(subregion_df, on='sigungu', how='left',)
    #         on_cols.append('sigungu')
    #         col_reorder.extend(['sigungu', '구'])
    #     elif 'sido' in k:
    #         subregion_df = load_sido_names()
    #         df = df.merge(subregion_df, on='sido', how='left',)
    #         on_cols.append('sido')
    #         col_reorder.extend(['sido', '시도'])
    #
    #     new[k] = df
    #
    # with pd.ExcelWriter(data_dir / 'indices_with_names.xlsx') as writer:
    #     for k, df in new.items():
    #         df.to_excel(writer, index=False, sheet_name=k.lower())
