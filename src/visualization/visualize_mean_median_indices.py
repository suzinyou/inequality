import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from ipywidgets import widgets
import plotly.express as px
from pathlib import Path
import plotly.io as pio
from src.env import project_dir, data_dir
from src.dictionary import translate, region2ko, unit2ko, var_color_map_tnsl, vargroup2ko


# def plot_lines(_df, y, key):
#     if 'sigungu' in key or 'sido' in key:
#         color = translate(key.split('_')[1])
#         vals = _df[color].unique()
#         color_map = {val: c for val, c in zip(vals, px.colors.qualitative.Plotly)}
#     else:
#         color_map = var_color_map
#         color = 'var'
#
#     if len(_df[color].unique()) >= 5:
#         height = 50 * 12
#     else:
#         height = 50 * 8
#
#     fig = px.line(_df, x="std_yyyy", y=y, color=color, line_dash=color,
#                   title=f"{translate(key)} {translate(y)}",
#                   width=50 * 12, height=height, color_discrete_map=color_map)
#     fig.update_traces(line=dict(width=2.4), mode='lines+markers')
#     fig.update_layout(margin={"r": 16, "t": 60, "l": 16, "b": 16})
#     fig.write_image(str(project_dir / "reports" / "figures" / f"{y}_{key}.png"), scale=2)

def is_money(v):
    if v.startswith('mean') or v.startswith('median'):
        return True
    return False


def get_format(v, tick=False):
    if is_money(v):
        if tick:
            return ",g"
        return "{x:,.0f}"
    elif v == 'gini':
        if tick:
            return ".3f"
        return "{x:.3f}"
    elif v == 'iqsr':
        if tick:
            return "g"
        return "{x:.1f}"
    elif v == 'rpr':
        if tick:
            return "%"
        return "{x:.1%}"
    elif v in ('count', 'num_indi', 'num_hh'):
        if tick:
            return ",g"
        return "{x:,}"
    else:
        raise ValueError(f"Unknown stat {v}")


def plot_lines(_df, stat, color_col, key, var=None, var_prefix_filter=None, facet_col=None, textlabel=False):
    df = _df.copy()

    if var_prefix_filter is not None:
        df = df[df['var'].apply(lambda x: x.startswith(var_prefix_filter))]

    if var is not None:
        df = df[df['var'] == var]

    unique_vars = df['var'].unique()
    if len(unique_vars) == 1 and facet_col == 'var':
        facet_col = None
        var = unique_vars[0]

    df['var'] = df['var'].apply(translate)

    if facet_col is not None:
        if facet_col not in _df.columns:
            raise ValueError(f"Facet column {facet_col} doesn't exist")
        facet_col = translate(facet_col)

    columns = list(set(_df.columns) - {'시도', 'sido', '구', 'sigungu'})
    df = df.rename(columns={k: translate(k) for k in columns})

    if 'sigungu' in key or 'sido' in key:
        color_col = translate(key.split('_')[1])
        trace_mode = 'lines'
    else:
        color_col = translate(color_col)
        trace_mode = 'lines+markers'

    if color_col == 'var':
        color_map = var_color_map_tnsl
    else:
        color_map = {v: c for v, c in zip(df[color_col].unique(), px.colors.qualitative.Plotly)}

    if 'sigungu' in key:
        height = 45 * 14
    elif len(df[color_col].unique()) >= 5:
        height = 45 * 12
    else:
        height = 45 * 8

    width = 50 * 12
    if facet_col is not None:
        width += 50 * 8 * (len(df[facet_col].unique()) - 1)

    title = f"{translate(key)} {translate(stat)}"
    new_stat = translate(stat)
    if var is not None:
        title += ' ' + translate(var)
        if var.startswith('inc') or var.startswith('prop'):
            new_stat += f' {translate(var)}'
            if is_money(stat):
                new_stat += '(천원)'
        else:
            new_stat = f'{translate(var)} ' + new_stat

    if is_money(stat):
        df[new_stat] = df[translate(stat)] / 1000
    else:
        df = df.rename(columns={translate(stat): new_stat})

    std_yyyy = translate("std_yyyy")

    if var_prefix_filter is not None and var is None:
        title += f" {vargroup2ko[var_prefix_filter]}"
    if facet_col is not None:
        title += f"({facet_col}별)"

    fig = px.line(df, x=std_yyyy, y=new_stat, color=color_col, line_dash=color_col,
                  width=width, height=height,
                  color_discrete_map=color_map,
                  facet_col=facet_col, facet_col_spacing=0.06)
    fig.update_traces(line=dict(width=2.4), mode=trace_mode)
    fig.update_layout(margin={"r": 16, "t": 24, "l": 36, "b": 16}, )
    fig.update_xaxes(**dict(tickmode='linear', tick0=2006, dtick=4))
    fig.update_yaxes(tickformat=get_format(stat, tick=True), matches=None)
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    if textlabel:
        cols = [std_yyyy, new_stat, color_col]
        if facet_col is not None:
            cols.append(facet_col)
        df_text = df.loc[
            df[std_yyyy].apply(lambda x: x in list(range(2006, 2019, 4))), cols].copy()
        df_text.loc[:, 'label'] = df[new_stat].apply(lambda x: get_format(stat).format(x=x))
        data = px.scatter(
            df_text, x=std_yyyy, y=new_stat, text='label', facet_col=facet_col
        ).update_traces(mode="text", textposition='top center')['data']

        def update(y):
            y.update(matches=None)
            y.showticklabels = True

        fig.for_each_yaxis(update)

        for trace in data:
            fig.add_trace(trace)

    # if 'sigungu' in key:
    #     fig.update_layout(showlegend=False)
    #     annotations = []
    #     mask = df[std_yyyy] == df[std_yyyy].max()
    #     label_df = df[mask]
    #     for y_trace, label in zip(label_df[new_stat], label_df['구']):
    #         # labeling the right_side of the plot
    #         # TODO: change to update at once~~
    #         annotations.append(dict(xref='paper', x=1.01, y=y_trace, xanchor='left', yanchor='middle', text=label, showarrow=False))
    #     fig.update_layout(annotations=annotations)

    fig.write_image(str(project_dir / "reports" / "figures" / f"{title}.png"), scale=2)

    return fig


def plot_population(_df, var, key):
    # Save line plot
    df = _df[_df['var'] == var].copy()
    if df.shape[0] == 0:
        raise ValueError(f"Can't find variable {var} in given df")
    cols = df.columns
    if 'num_indi' in cols:
        y1 = 'num_indi'
    elif 'num_hh' in cols:
        y1 = 'num_hh'
    else:
        return None

    fig = px.bar(df, x="std_yyyy", y=y1,
                 title=f"{translate(key)} {translate(y1)}", opacity=0.7,
                 width=50 * 12, height=50 * 8, )

    if 'adult' in key:
        fig.add_trace(go.Bar(x=df.std_yyyy, y=df['count'], opacity=0.7, name='소득자수', base=0))

    fig.update_layout(margin={"r": 16, "t": 16, "l": 24, "b": 16})
    fig.write_image(str(project_dir / "reports" / "figures" / f"count_{var}_{key}.png"), scale=2)


if __name__ == "__main__":
    mmi = pd.read_excel(data_dir / 'mean_median_and_indices.xlsx',
                        sheet_name=None)

    # combine dfs if there's KR & SEOUL
    both_regions = []
    one_region = []
    for k in mmi.keys():
        suffix = '_'.join(k.split('_')[1:])

        if k.startswith('kr'):
            prefix = 'seoul'
        else:
            prefix = 'kr'

        other_key = '_'.join([prefix, suffix])

        if other_key in mmi.keys():
            both_regions.append(suffix)
        else:
            one_region.append(k)

    dfs = {}
    for suffix in both_regions:
        kr = mmi['kr_' + suffix]
        kr['region'] = '전국'
        seoul = mmi['seoul_' + suffix]
        seoul['region'] = '서울'
        dfs[suffix] = pd.concat([kr, seoul]).reset_index(drop=True)

    for key in one_region:
        dfs[key] = mmi[key]

    for k, df in dfs.items():
        # TODO: remove this code after debugging!
        if 'sigungu' not in k:
            continue

        df = df[df['std_yyyy'] >= 2006]

        if 'sigungu' in k or 'sido' in k:
            subregunit = k.split('_')[1]
            subreg_notnull = df[subregunit].notnull()
            df = df[subreg_notnull]

            for var in df['var'].unique():
                fig = plot_lines(df, stat='mean_real', key=k, var=var, color_col=subregunit, facet_col='var',
                                 textlabel=False)
                fig = plot_lines(df, stat='median_real', key=k, var=var, color_col=subregunit, facet_col='var',
                                 textlabel=False)
        elif k in both_regions:
            if 'earner' in k:
                for var in df['var'].unique():
                    fig = plot_lines(df, stat='mean_real', key=k, var=var, color_col='region', facet_col='var',
                                     textlabel=False)
                    fig = plot_lines(df, stat='median_real', key=k, var=var, color_col='region', facet_col='var',
                                     textlabel=False)
            else:
                for var_group in ['inc', 'prop_txbs']:
                    if df['var'].apply(lambda x: not x.startswith(var_group)).all():
                        continue
                    plot_lines(df, stat='mean_real', key=k, var_prefix_filter=var_group, color_col='region', facet_col='var',
                               textlabel=True)
                    plot_lines(df, stat='median_real', key=k, var_prefix_filter=var_group, color_col='region', facet_col='var',
                               textlabel=True)

                    if 'gini' in df.columns:
                        fig = plot_lines(df, stat='gini', key=k, var_prefix_filter=var_group, color_col='region', facet_col='var',
                                         textlabel=True)
                        if not df['iqsr'].isnull().all():
                            fig = plot_lines(df, stat='iqsr', key=k, var_prefix_filter=var_group, color_col='region', facet_col='var',
                                             textlabel=True)
                        fig = plot_lines(df, stat='rpr', key=k, var_prefix_filter=var_group, color_col='region', facet_col='var',
                                         textlabel=True)

        else:
            for var_group in ['inc', 'prop_txbs']:
                if df['var'].apply(lambda x: not x.startswith(var_group)).all():
                    continue
                fig = plot_lines(df, stat='mean_real', key=k, var_prefix_filter=var_group, color_col='var', facet_col='var')
                fig = plot_lines(df, stat='median_real', key=k, var_prefix_filter=var_group, color_col='var', facet_col='var')

        # if 'num_indi' in df.columns or 'num_hh' in df.columns:
        #     for vname in df['var'].unique():
        #         plot_population(df, vname, k)

