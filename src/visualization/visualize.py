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
from src.dictionary import translate, region2ko, unit2ko


def plot_lines(df, y, title, save_name):
    # Save line plot
    fig = px.line(df, x="std_yyyy", y=y, color='var', line_dash='var',
                  title=title,
                  width=50 * 12, height=50 * 8)
    fig.update_traces(line=dict(width=2.4))
    fig.update_layout(margin={"r": 16, "t": 60, "l": 16, "b": 16})
    fig.write_image(project_dir / "reports" / "figures" / save_name)


def save_lorenz_curve(df, unit, year, var):
    filter_mask = (df['std_yyyy'] == year) & (df['var'] == var)
    _df = df[filter_mask]

    gini = (0.5 - np.sum(_df.cumshare * _df['freq'] / _df['year_count'])) / 0.5
    t1 = go.Bar(x=_df['freq'].cumsum() / _df['year_count'],
                y=_df['cumshare'], name="Lorenz Curve")
    t2 = go.Scatter(x=[0, 1.], y=[0, 1.], name="perfect equality", )  # fill='tonexty')

    fig = go.Figure(data=[t1, t2],
                    layout=go.Layout(
                        xaxis=dict(title='누적인구비율'),
                        yaxis=dict(title='누적점유율'),
                        title=dict(
                            text=f'{year}년 서울 {translate(var)} Lorenz Curve (~100분위, Gini={gini:.3f})'
                        ),
                        height=700, width=840))
    fig.write_image(project_dir / 'reports' / 'figures' / f"lorenz_curve_approx_{unit}-{year}-{var}.png")


def save_dist_chart(df, var):
    spl = var.split('_')
    stat = spl[0]
    vname = '_'.join(spl[1:])
    fig = px.bar(df[df.STD_YYYY.apply(lambda x: x in (2006, 2010, 2014, 2018))],
                 x="age_group", y=var,
                 facet_col='STD_YYYY', facet_col_wrap=2,
                 title=f"연령대별 {translate(stat)} {translate(vname)}",
                 width=100 * 12, height=100 * 8)
    # fig.update_traces(line=dict(width=2.4))
    fig.update_layout(
        margin={"r": 16, "t": 60, "l": 16, "b": 16})
    fig.write_image(f"../reports/figures/agegroup-{var}.png")


def plot_shares_stacked(df, key, var):
    """ income_shares.xlsx """
    colors = px.colors.qualitative.Plotly
    pastels = px.colors.qualitative.Pastel1
    color_discrete_map = {
        'Bottom 50%': colors[0],
        'Middle 40%': colors[2],
        'Top 10%': colors[1],
        'Bottom 20%': pastels[1],
        'Top 1%': pastels[0]
    }

    fig = px.bar(
        df[(df['var'] == var) & (df['income_group'].apply(lambda x: x in ['Bottom 50%', 'Middle 40%', 'Top 10%']))],
        x="std_yyyy", y="share", color="income_group",
        title=f"{translate(key)} {translate(var)} 점유율",
        color_discrete_map=color_discrete_map,
        width=50 * 12, height=50 * 8
    )

    bottom20 = df[(df['var'] == var) & (df['income_group'] == 'Bottom 20%')]
    fig.add_trace(go.Scatter(x=bottom20.std_yyyy, y=bottom20.share,
                             mode='lines+markers',
                             name='Bottom 20%',
                             line=dict(color=pastels[1]),
                             marker=dict(color=pastels[1])))

    top1 = df[(df['var'] == var) & (df['income_group'] == 'Top 1%')].copy()
    top1['share'] = 1 - top1['share']
    fig.add_trace(go.Scatter(x=top1.std_yyyy, y=top1.share,
                             mode='lines+markers',
                             name='Top 1%',
                             line=dict(color=pastels[0]),
                             marker=dict(color=pastels[0])))
    path_obj = project_dir / 'reports' / 'figures' / f"share_stacked_{key}-{var}.png"
    fig.write_image(str(path_obj))
    return fig


def plot_shares_line(df, key, var):
    """ income_shares.xlsx """
    df_filtered = df[df['var'] == var]
    fig = px.line(df_filtered, x="std_yyyy", y="share", color='income_group', line_dash='income_group',
                  title=f"{translate(key)} {translate(var)} 점유율",
                  facet_col='var',
                  width=50 * 12, height=50 * 12)
    fig.update_traces(line=dict(width=2.4), mode='lines+markers')
    fig.update_layout(margin={"r": 16, "t": 60, "l": 16, "b": 16})
    path_obj = project_dir / 'reports' / 'figures' / f"share_line_{key}-{var}.png"
    fig.write_image(str(path_obj))
    return fig


if __name__ == "__main__":
    pass
    # df = pd.read_csv(data_dir / '02_mean_median' / 'seoul_agegroup.csv')
    # for v in ['inc_wage', 'inc_tot', 'prop_txbs_hs']:
    #     save_dist_chart('mean_'+v)
