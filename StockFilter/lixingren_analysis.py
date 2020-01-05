#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import requests
import logging
import pandas
import json
import datetime
from dateutil.parser import parse
from pandas import ExcelWriter
from dotenv import load_dotenv
from pathlib import Path

# NOTE: Need to update the list when the time changes.
INCREAMENT_RATES = [
    '净利润同比增长率(%)2019.09.30',
    '净利润同比增长率(%)2018.12.31',
    '营业收入(同比增长率)(%)2019.09.30',
    '营业收入(同比增长率)(%)2018.12.31'
];

def sortByYear(data):
    date = parse(data['standardDate'])
    return date.year

def chain_parse(obj, chain):
    els = chain.split('.')
    try:
        value = obj
        for el in els:
            value = value[el]
        return value
    except Exception:
        return 0

def evaluate_stats(res):
    data = res['data']
    print(list(filter(lambda x: x.get('reportType', '') == 'annual_report', data)))

    print('LLL')
    annual_list = list( filter(lambda x:x.get('reportType', '') == 'annual_report', data) )
    annual_list.sort(key=sortByYear)

    # "h_y.profitStatement.oi.t",
    # "h_y.balanceSheet.ar.t",
    # "h_y.balanceSheet.i.t",
    # "h_y.balanceSheet.tca_tcl_r.t",
    # "h_y.profitStatement.np.t",
    # "h_y.metrics.fcf.t",
    # "h_y.cashFlow.ncffoa.t"

    # 计算往年 应收账款，存货以及营业收入的 增长值。按年份从低到高排序。
    # 营业收入
    earning_list = list( map( lambda x: chain_parse(x, f'h_y.profitStatement.oi.t'), annual_list) )
    print('营业收入', earning_list)

    # 应收账款
    receivables_list = list( map( lambda x: chain_parse(x, f'h_y.balanceSheet.ar.t'), annual_list) )
    print('应收账款', receivables_list)

    # 流动比率
    tca_tcl_list = list( map( lambda x: chain_parse(x, f'h_y.balanceSheet.tca_tcl_r.t'), annual_list) )
    print('流动比率', tca_tcl_list)

    # 存货
    inventory_list = list( map( lambda x: chain_parse(x, f'h_y.balanceSheet.i.t'), annual_list) )
    print('存货', inventory_list)

    # 自由现金流
    fcf_list = list( map( lambda x: chain_parse(x, f'h_y.metrics.fcf.t'), annual_list) )
    print('自由现金流', fcf_list)

    # 现金流量净额
    ncffoa_list = list( map( lambda x: chain_parse(x, f'h_y.cashFlow.ncffoa.t'), annual_list) )
    print('现金流量净额', ncffoa_list)

    earning_diff = []
    receivables_diff = []
    inventory_diff = []

    # Calculate diff.
    for i in range(1, len(annual_list)):
        earning_diff.append(earning_list[i] - earning_list[i - 1])
        receivables_diff.append(receivables_list[i] - receivables_list[i - 1])
        inventory_diff.append(inventory_list[i] - inventory_list[i - 1])

    print('earning_diff', earning_diff)
    print('receivables_diff', receivables_diff)
    print('inventory_diff', inventory_diff)

    receivables_over_flag = False
    inventory_over_flag = False
    tca_tcl_over1_flag = all( rate >= 1 for rate in tca_tcl_list )

    for i in range(1, len(earning_diff)):
        receivables_over_flag = receivables_over_flag or ( receivables_diff[i - 1] > earning_diff[i - 1] and receivables_diff[i] > earning_diff[i])
        inventory_over_flag = inventory_over_flag or ( inventory_diff[i - 1] > earning_diff[i - 1] and inventory_diff[i] > earning_diff[i])

    print('receivables_over_flag', receivables_over_flag)
    print('inventory_over_flag', inventory_over_flag)
    print('tca_tcl_over1_flag', tca_tcl_over1_flag)

    return (receivables_over_flag, inventory_over_flag, tca_tcl_over1_flag, fcf_list, ncffoa_list)

def getFundamentalStats(code):
    standard_code = code.split('.')[0]

    today = datetime.date.today()
    weekday_delta = today.weekday() - 4
    if weekday_delta > 0:
        today = today - datetime.timedelta(days=weekday_delta)

    data = {
        "token": os.environ['API_TOKEN'],
        "date": str(today),
        "stockCodes": [
           standard_code
        ],
        "metrics": [
            "d_pe_ttm_pos10",
            "pb_wo_gw_pos10"
        ]
    }
    res = requests.post(
        f'https://open.lixinger.com/api/a/stock/fundamental',
        json=data,
        headers={'Content-Type': 'application/json'}
    )
    return res.json()


def getStatsForStock(code):
    standard_code = code.split('.')[0]
    data = {
        "token": os.environ['API_TOKEN'],
        "startDate": "2015-12-01",
        "endDate": "2019-01-30",
        "stockCodes": [
           standard_code
        ],
        "metrics": [
            "h_y.profitStatement.oi.t",
            "h_y.balanceSheet.ar.t",
            "h_y.balanceSheet.i.t",
            "h_y.balanceSheet.tca_tcl_r.t",
            "h_y.profitStatement.np.t",
            "h_y.metrics.fcf.t",
            "h_y.cashFlow.ncffoa.t"
        ]
    }

    res = requests.post(
        f'https://open.lixinger.com/api/a/stock/fs/industry',
        json=data,
        headers={'Content-Type': 'application/json'}
    )
    print(res.json())
    return res.json()

def filter_by_increment_rate(dataframe):
    filtered_list = []
    for index, row in dataframe.iterrows():
        if float( row['净利润同比增长率(%)2019.06.30'] ) > 0 \
                and float( row['净利润同比增长率(%)2018.12.31'] ) > 0 \
                and float( row['营业收入(同比增长率)(%)2019.06.30'] ) > 0 \
                and float( row['营业收入(同比增长率)(%)2018.12.31'] ) > 0:
            filtered_list.append(row.values)

    new_df = pandas.DataFrame(filtered_list, columns=dataframe.columns)
    return new_df

def filter_by_rules(df):
    filtered_list = []
    for index, row in df.iterrows():
        print(f'####### Stock: {row["股票简称"]} {row["股票代码"]}')
        res = getStatsForStock( row['股票代码'] )
        (receivables_over_flag, inventory_over_flag, tca_tcl_over1_flag, *rest ) = evaluate_stats(res)

        if not receivables_over_flag and not inventory_over_flag and tca_tcl_over1_flag:
            filtered_list.append(row.values)
            print('SUCCESS: {row["股票简称"]} {row["股票代码"]}')
        else:
            print(f'FAIL: {row["股票简称"]} {row["股票代码"]}', receivables_over_flag, inventory_over_flag, tca_tcl_over1_flag)

    new_df = pandas.DataFrame(filtered_list, columns=df.columns)

    print(new_df.shape)
    return new_df

def filter_by_fundamental(df):
    cols = list( df.columns.values )
    cols.append('PE-TTM(扣非)分位点(10年)')
    cols.append('PB(不含商誉)分位点(10年)')

    filter_list = []
    for index, row in df.iterrows():
        res = getFundamentalStats( row['股票代码'] )
        data = res['data'][0]
        pe = float(data['d_pe_ttm_pos10']) * 100
        pb = float(data['pb_wo_gw_pos10']) * 100
        print(f"{row['股票简称']} PE: {pe}  PB: {pb}")
        if (pe <= 50 and pb <= 50 ):
            concat = list(row.values) + list([ pe, pb ])
            filter_list.append( concat )
    new_df = pandas.DataFrame(filter_list, columns=cols)
    return new_df

def clean_data(df):
    required_fields = [
                  '股票代码', '股票简称', '现价(元)', '涨跌幅(%)',
                  '上市日期', '所属同花顺行业', *INCREAMENT_RATES
              ]
    return df.loc[:, list(required_fields)]

#
# if __name__ == '__main__':
#     env_path = f'{Path(__file__).parents[1]}/.env'
#     load_dotenv(dotenv_path=env_path)
#
#     # df = pandas.read_excel('output.xlsx')
#     # df.sort_values(by='PE-TTM(扣非)分位点(10年)', inplace=True)
#     # print(df)
#     # df.to_excel('output.xlsx')
#
#     # 1. Read data.
#     df = pandas.read_html('2020-01-06.html')[0]
#
#     # 2. Reformat data.
#     df.columns = df.head(1).values[0]
#     df = df.drop([0])
#     df = df.reset_index(drop=True)
#
#     df = clean_data(df)
#
#     print(df.head(2))
#     print(df.columns)
#
#     print(df.columns.values)
    #
    # print(f'**** {df.shape[0]} stocks.')
    #
    # print('Filtering by increment rate...')
    # df = filter_by_increment_rate(df)
    # print(f'End with {df.shape[0]} stocks.')
    #
    # print('Filtering by rules...')
    # df = filter_by_rules(df)
    # print(f'End with {df.shape[0]} stocks.')
    #
    # print('Filtering by fundamental stats...')
    # df = filter_by_fundamental(df)
    # print(f'End with {df.shape[0]} stocks.')
    #
    # print('Sort by PE.')
    # df.sort_values(by='PE-TTM(扣非)分位点(10年)', inplace=True)
    #
    # df.to_excel('output.xlsx')