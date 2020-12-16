import pandas as pd

from src.env import data_dir


def load_cpi(region):
    """Load consumer price index data"""
    cpi = pd.read_excel(data_dir / 'external' / '소비자물가지수_서울과 전국__20201113113326.xlsx',
                        index_col='시도별(2)').T.iloc[2:]
    cpi /= 100
    cpi = cpi.reset_index().rename(
        columns={'index': 'std_yyyy', '소계': '전국', '서울특별시': '서울'}
    ).astype(
        {'std_yyyy': int, '전국': float, '서울': float}
    )
    if region == '서울':
        return cpi.drop(['전국'], axis=1).rename(columns={'서울': 'cpi'})
    if region == '전국':
        return cpi.drop(['서울'], axis=1).rename(columns={'전국': 'cpi'})
    return cpi


def load_sido_names():
    regcd = pd.read_csv(data_dir / 'external' / '(양식)customized_DB_application_200707' / '코드설명_동읍면-Table 1.csv')
    regcd = regcd[['SIDO_CD', 'SIDO_NM']].groupby('SIDO_CD').first().reset_index().iloc[:-1]
    regcd = regcd.rename(columns={'SIDO_CD': 'sido', 'SIDO_NM': '시도'})
    regcd.sido = regcd.sido.astype(int)
    return regcd


def load_sigungu_names():
    regcd = pd.read_csv(data_dir / 'external' / '(양식)customized_DB_application_200707' / '코드설명_동읍면-Table 1.csv')
    seoul_mask = regcd['SIDO_SGG_CD'].apply(lambda x: str(x).startswith('11'))
    sigungudf = regcd.loc[seoul_mask, ['SIDO_SGG_CD', 'SGG_NM']].groupby('SIDO_SGG_CD').first().reset_index()
    sigungudf['SIDO_SGG_CD'] = sigungudf['SIDO_SGG_CD'].apply(lambda x: int(x) % 1000)
    sigungudf = sigungudf.rename(columns={'SIDO_SGG_CD': 'sigungu', 'SGG_NM': '구'})
    return sigungudf