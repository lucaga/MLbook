# the following magic code allows visual studio to output graphics
# it does so by using ipympl
# %matplotlib ipympl
# %matplotlib widget
# unfortunately, it doesn't work

import json
import logging

# Adding # noqa to a line indicates that the linter 
# (a program that automatically checks code quality) 
# should not check this line. 
# Any warnings that code may have generated will be ignored.

import hvplot.polars  # noqa 
import polars as pl


from pydantic import BaseModel


from degiro_connector.quotecast.tools.chart_fetcher import ChartFetcher, SeriesFormatter
from degiro_connector.quotecast.models.chart import ChartRequest, Interval

logging.basicConfig(level=logging.INFO)

with open("../Courses/PythonExercises/config/config.json") as config_file:
    config_dict = json.load(config_file)

# FETCH CHART
user_token = config_dict.get("user_token")  # HERE GOES YOUR USER_TOKEN
chart_fetcher = ChartFetcher(user_token=user_token)
chart_request = ChartRequest(
    culture="fr-FR",
    # override={
    #     "resolution": "P1D",
    #     "period": "P1W",
    # },
    period=Interval.P1D,
    requestid="1",
    resolution=Interval.PT60M,
    series=[
        "issueid:360148977",
        "price:issueid:360148977",
        "ohlc:issueid:360148977",
        "volume:issueid:360148977",
        # "vwdkey:AAPL.BATS,E",
        # "price:vwdkey:AAPL.BATS,E",
        # "ohlc:vwdkey:AAPL.BATS,E",
        # "volume:vwdkey:AAPL.BATS,E",
    ],
    tz="Europe/Paris",
)
chart = chart_fetcher.get_chart(
    chart_request=chart_request,
    raw=False,
)
print("first table")
if isinstance(chart, BaseModel):
    for series in chart.series:
        print(pl.DataFrame(data=series.data, orient="row"))

print("second table")
if isinstance(chart, dict):
    print(chart)

print("third table - formatted with names")
if chart:
    for series in chart.series:
        if series.times is None or series.type not in ["time", "ohlc"]:
            print("this part of the data is an ",series.type,", sorry cannot print")
        else :
            df = SeriesFormatter.format(series=series)
            print(df)
            print(type(df))
            # df_formatted = pl.DataFrame(df)
            # print(type(df_formatted))
            df.hvplot.line()

