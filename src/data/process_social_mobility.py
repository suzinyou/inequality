import pandas as pd
import re
import numpy as np
from src.env import data_dir


filename = 'social_mobility-1and2_multi_dependent'

if __name__ == "__main__":

    sm = pd.read_excel(
        data_dir / '08_사회이동성' / f"{filename}.xlsx", sheet_name=None, skiprows=[0])

    sm_sheet_names = {k.lower(): k for k in sm.keys()}
    new = dict()

    for k, df in sm.items():
        if len(re.findall(r'[\u1100-\u11FF\uac00-\uD7AF]', k)) == 0:
            # 한글 제목만
            continue

        df = df.rename(columns={'Unnamed: 0': '연도'})
        if df['Unnamed: 2'].iloc[1:].isnull().all():
            df = df.set_index(['연도'])
            stat = df['Unnamed: 1'][df['Unnamed: 1'].notnull()].iloc[-1]
            df = df.drop(columns=['Unnamed: 1', 'Unnamed: 2'])
            df = df.iloc[1:]
            df = df.T
        else:
            df = df.rename(columns={'Unnamed: 1': '성별'})
            d = {'male': '남성', 'female': '여성'}
            df['성별'] = df['성별'].apply(lambda x: d[x] if x in d else x)
            df['연도'] = df.연도.ffill()
            df = df.set_index(['연도', '성별'])
            stat = df['Unnamed: 2'][df['Unnamed: 2'].notnull()].iloc[-1]
            df = df.drop(columns=['Unnamed: 2'])
            df = df.iloc[1:]
            df = df.stack().swaplevel(1, 2).unstack(level=0)

        cols = df.infer_objects().select_dtypes('float').columns
        if stat.startswith('count') or stat.startswith('mean'):
            df[cols] = np.around(df[cols] / 10000, 0).astype(int)
        elif stat.startswith('frac'):
            df[cols] = np.around(df[cols] * 100, 0).astype(int)

        deciles = df.columns
        col_update = {}
        for i, d in enumerate(deciles):
            if i < len(deciles) - 1 and d + 1 < deciles[i+1]:
                col_update[d] = f"{d}-{deciles[i + 1]-1}분위"
            else:
                col_update[d] = f"{d}분위"

        df = df.rename(columns=col_update)

        new[k] = df

    with pd.ExcelWriter(data_dir / '08_사회이동성' / f'{filename}_formatted.xlsx') as writer:
        for k, df in new.items():
            df.to_excel(writer, sheet_name=k)
