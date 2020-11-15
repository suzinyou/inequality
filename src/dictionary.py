dic = {}

region2ko = {
    'seoul': '서울',
    'kr': '전국',
    'krpanel': '전국패널'
}

stat2ko = {
    'mean': '평균',
    'median': '중위',
    'max': '최대',
    'min': '최소',
    'std': '표준편차',
    'count': '',
}

var2ko = {
    'inc_tot': '총소득',
    'inc_wage': '근로소득',
    'inc_bus': '사업소득',
    'inc_int': '이자소득',
    'inc_divid': '배당소득',
    'inc_pnsn_natl': '국민연금소득',
    'inc_pnsn_occup': '직역연금소득',
    'prop_txbs_tot': '총재산과세표준',
    'prop_txbs_hs': '주택과세표준',
    'prop_txbs_lnd': '토지과세표준',
    'prop_txbs_bldg': '건물과세표준',
    # 'prop_txbs_shp': '선박항공기과세표준',
}
unit2ko = {
    'indi': '개인',
    'hh': '가구',
    'eq': '균등화',
}

dic.update(region2ko)
dic.update(stat2ko)
dic.update(var2ko)
dic.update(unit2ko)


def translate(x):
    if x not in dic:
        raise ValueError(f"Can't translate {x}!")
    return dic[x]
