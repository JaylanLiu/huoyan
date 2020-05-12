import dash
import dash_table
import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
import dash_auth

import os
import sys
import re
import argparse
import numpy as np
import pandas as pd
from collections import defaultdict
import json
import sqlite3
import logging
import time 
import yaml
import datetime
from pathlib import Path
import importlib

# configure relative imports if running as a script; see PEP 366
if __name__ == "__main__" and __package__ is None:
    # replace the script's location in the Python search path by the main
    # scripts/ folder, above it, so that the importer package folder is in
    # scope and *not* directly in sys.path; see PEP 395
    sys.path[0] = str(Path(sys.path[0]).resolve().parent)
    __package__ = 'HuoYan_monitoring'
    # explicitly import the package, which is needed on CPython 3.4 because it
    # doesn't include https://github.com/python/cpython/pull/2639
    importlib.import_module(__package__)

from .HuoYan_monitoring import HuoYan_monitoring

# v0.1,不提供自动刷新，需要手工刷新以保证显示最新结果

# argparse
parser = argparse.ArgumentParser(description='HuoYan laboratory COVID-19 samples testing lifetime monitoring')
parser.add_argument('--db',
                        type=str, 
						help='sqlite3 database file path, create if not exists',
                        default=r'HuoYan_records.db')
parser.add_argument('--config',
                        type=str, 
						help='path of config file, yaml format',
                        default=r'config.yaml')
parser.add_argument('--auth',
                        type=str,
						help='path of file contains user and passwd in yaml format',
                        default=r'auth.yaml')
parser = parser.parse_args()    


# load config
if not os.path.exists(parser.config):
	raise ValueError('无效的config文件',parser.config)

if not os.path.exists(parser.auth):
	raise ValueError('无效的auth文件',parser.config)

with open(parser.config, 'r', encoding='utf-8') as f:
    config = yaml.load(f.read())

today=str(datetime.datetime.now()).split(' ')[0]

with open(parser.auth, 'r', encoding='utf-8') as f:
    auth_info = yaml.load(f.read())
    auth_info = [[x,auth_info[x]] for x in auth_info.keys()]

#app
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

#auth
auth = dash_auth.BasicAuth(
	app,
	auth_info
)

# layout
app.layout = html.Div(children=[
	html.H1(f"{config['laboratory_ch']}全流程监控系统\n"),
	html.H1(f"{config['laboratory']} COVID-19 Test Monitoring"),

	html.Button(id='submit-button', n_clicks=0, children='刷新'),
	
	html.Div([# static whole picture state
		html.H2('Statistics'),
		html.Div(id='statistics'),
		# here is the preparation location for statistics figures
	]), 

	html.Div([# 在检样本详情
		html.H2('在检样本详情'),
		dcc.RadioItems(
			id='category',
			options=[
				{'label': '有样无单', 'value': 'no_info'},
				{'label': '有单无样', 'value': 'no_sample'},
				{'label': '未发报告', 'value': 'no_report'}
			],
			value='no_report',
			labelStyle={'display': 'inline-block'}
		),
		dash_table.DataTable(
			id='table',
		),
	]),
	html.Footer('CopyrightⒸ BGI 2020 版权所有 深圳华大基因股份有限公司 all rights reserved. '),
	
])

# 用于刷新数据
@app.callback(
	dash.dependencies.Output('statistics','children'),
	[dash.dependencies.Input('submit-button', 'n_clicks')]
)
def get_statistics(n_clicks):
	hy = HuoYan_monitoring(configfile=parser.config)
	try:
		hy.collect_infos()
		statistics = ''
	except Exception as e:
		statistics = e
		print(e)
	df=pd.read_sql('select * from test_lifetime',con=hy.con)

	today_test_df=df[df['test'].str.contains(today).fillna(False)] # 今天到样
	today_report_df=df[df['report'].str.contains(today).fillna(False)] # 今天报告

	statistics += f'''
	当日到样：{len(today_test_df)}\t当日已发报告：{len(today_report_df)}\n\n
	累计到样：{len(df)}\t累计发送报告：{len(df[df['report'].notnull()])}\t累计异常结束：{len(df[df['finished'].notnull() & df['exception'].notnull()])}\t检测中：\t{len(df[df['finished'].isnull()])}\n

	'''
	return statistics


# table columns
@app.callback(
	dash.dependencies.Output('table','columns'),
	[dash.dependencies.Input('submit-button', 'n_clicks')]
)
def get_table_columns(n_clicks):
	hy = HuoYan_monitoring(configfile=parser.config)
	df=pd.read_sql('select * from test_lifetime',con=hy.con)

	return [{"name": i, "id": i} for i in df.columns]

# table data
@app.callback(
	dash.dependencies.Output('table','data'),
	[dash.dependencies.Input('submit-button', 'n_clicks'),
	dash.dependencies.Input('category','value'),
	dash.dependencies.Input('statistics','children')]
)
def get_table_data(n_clicks,cate_value,state):
	hy = HuoYan_monitoring(configfile=parser.config)
	df=pd.read_sql('select * from test_lifetime',con=hy.con)
	
	if cate_value == 'no_info':
		df=df[df.test.notnull() & df['sample'].isnull() & df.finished.isnull()]
	elif cate_value == 'no_sample':
		df=df[df.finished.isnull() & df['sample'].notnull() & df['test'].isnull() & df['exception'].isnull()]
	elif cate_value == 'no_report':
		df=df[df.finished.isnull() & df['sample'].notnull() & df.test.notnull() & df.report.isnull()]
	else:
		raise ValueError('不支持的类别',cate_value)

	return df.to_dict("rows")




if __name__ == '__main__':
    #app.run_server(debug=True,port=8080)
	# for production environment, debug must be False
	app.run_server(debug=False,port=8080)