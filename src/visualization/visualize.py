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
    t2 = go.Scatter(x=[0, 1.], y=[0, 1.], name="perfect equality",)# fill='tonexty')

    fig = go.Figure(data=[t1, t2],
                    layout=go.Layout(
                    xaxis=dict(title='누적인구비율'),
                    yaxis=dict(title='누적점유율'),
                    title=dict(
                        text=f'{year}년 서울 {translate(var)} Lorenz Curve (~100분위, Gini={gini:.3f})'
                    ), height=700, width=840))
    fig.write_image(f"../reports/figures/lorenz_curve_approx_{unit}-{year}-{var}.png")

if __name__ == "__main__":
    unit = 'indi'
    gini_name = f"Approximated Gini coefficient ({translate(unit)})"
    gini_save_name = f"lorenz_curve_approx_{unit}.png"

    income_groups = [
        'Bottom 20%',
        'Next 30%',
        'Bottom 50%',
        'Middle 40%',
        'Top 10%',
        'Top 1%',
        'Top 0.1%']

    for g in income_groups:
        fig = px.line(res[res['income_group'] == g], x="std_yyyy", y="share", color='var', line_dash='var',
                      title=f"{g}",
                      width=50 * 12, height=50 * 8)
        fig.update_traces(line=dict(width=2.4))
        fig.update_layout(margin={"r": 16, "t": 72, "l": 16, "b": 16}, plot_bgcolor='rgba(240,240,250,1)')
        fig.show()