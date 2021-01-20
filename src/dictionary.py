import plotly.express as px


dic = {}

region2ko = {
    'seoul': '서울',
    'seoulpanel': '서울패널',
    'kr': '전국',
    'krpanel': '전국패널',
}

stat2ko = {
    'mean': '평균',
    'median': '중위',
    'max': '최대',
    'min': '최소',
    'std': '표준편차',
    'count': '해당인구',
    'num_indi': '인구',
    'num_hh': '가구',
    'frac_earners': '소득자 비율',
    'gini': '지니계수',
    'iqsr': '5분위배율',
    'rpr': '상대빈곤율'
}

var2ko = {
    'inc_tot': '총소득',
    'inc_wage': '근로소득',
    'inc_bus': '사업소득',
    'inc_int': '이자소득',
    'inc_divid': '배당소득',
    'inc_othr': '기타소득',
    'inc_pnsn_natl': '국민연금소득',
    'inc_pnsn_occup': '직역연금소득',
    'inc_fin': '금융소득',
    'inc_pnsn': '공적연금소득',
    'inc_main': '본원소득(총-연금)',
    'prop_txbs_tot': '총재산과세표준',
    'prop_txbs_hs': '주택과세표준',
    'prop_txbs_lnd': '토지과세표준',
    'prop_txbs_bldg': '건물과세표준',
    # 'prop_txbs_shp': '선박항공기과세표준',
}
unit2ko = {
    'indi': '개인',
    'hh1': '주민등록세대',
    'hh2': '재편 가구',
    'eq': '균등화',
    'eq1': '주민등록세대 균등화',
    'eq2': '재편가구 균등화',
    'adult20': '성인(20세 이상)',
    'adult60': '노인(60세 이상)',
    'adult15': '15세 이상',
    'capita': '전',
    'earner': '소득자',
    'sigungu': '구',
    'sido': '시도'
}

col2ko = {
    'var': '변수',
    'std_yyyy': '연도',
    'region': '지역',
    'income_group': '소득구간',
    'share': '점유율'
}

dic.update(region2ko)
dic.update(stat2ko)
dic.update(var2ko)
dic.update(unit2ko)
dic.update(col2ko)
dic.update(real='실질')


vargroup2ko = {
    'inc': '소득',
    'prop_txbs': '재산세과세표준'
}


def translate(x):
    x = x.lower()
    if x in dic:
        return dic[x]

    split = x.split('_')
    if len(split) == 0:
        raise ValueError(f"Got empty input {x}")
    elif len(split) == 1:
        raise ValueError(f"Can't translate {x}!")
    else:
        for i in range(1, len(split) + 1):
            cur_key = '_'.join(split[:i])
            if cur_key in dic:
                return dic[cur_key] + ' ' + translate('_'.join(split[i:]))
        raise KeyError(f"Can't translate {x}!")


colors = px.colors.qualitative.Plotly
tab10 = px.colors.qualitative.T10
var_color_map = {
    'inc_tot': colors[0],
    'inc_wage': colors[2],
    'inc_bus': colors[3],
    'inc_fin': colors[4],
    'inc_pnsn': colors[5],
    'inc_main': colors[6],
    'prop_txbs_tot': colors[1],
    'prop_txbs_hs': tab10[0],
    'prop_txbs_lnd': tab10[1],
    'prop_txbs_bldg': tab10[2],
}
var_color_map_tnsl = {}
for k, v in var_color_map.items():
    var_color_map_tnsl[translate(k)] = v