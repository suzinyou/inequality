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
    fig.update_layout(margin={"r": 16, "t": 72, "l": 16, "b": 16})
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
    fig.write_image(f"../reports/figures/lorenz_curve_approx_{unit}-{year}-{var}.png")


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
        margin={"r": 16, "t": 72, "l": 16, "b": 16})
    fig.write_image(f"../reports/figures/agegroup-{var}.png")


if __name__ == "__main__":
    pass
    # df = pd.read_csv(data_dir / '02_mean_median' / 'seoul_agegroup.csv')
    # for v in ['inc_wage', 'inc_tot', 'prop_txbs_hs']:
    #     save_dist_chart('mean_'+v)
