import dash
import dash_table
import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
import dash_auth
import base64

import os
import sys
import re
import io
import argparse
import numpy as np
import pandas as pd
from collections import defaultdict
import json
import sqlite3
import logging
import time
import urllib 
import yaml
import datetime
import threading
from apscheduler.schedulers.blocking import BlockingScheduler

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
from .qPCR_parser import qPCR_parser
from .sample_info_validate import validate

# v0.1,不提供自动刷新，需要手工刷新以保证显示最新结果
# v0.2, providing auto refresh using threading and apscheduler

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
    config = yaml.load(f.read(),Loader=yaml.FullLoader)

today=str(datetime.datetime.now()).split(' ')[0]

with open(parser.auth, 'r', encoding='utf-8') as f:
    auth_info = yaml.load(f.read(),Loader=yaml.FullLoader)
    auth_info = [[x,auth_info[x]] for x in auth_info.keys()]

#app
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

#auth
auth = dash_auth.BasicAuth(
	app,
	auth_info
)


# crontab tasks
hy = HuoYan_monitoring(configfile=parser.config)
hy.collect_infos()
df=pd.read_sql('select * from test_lifetime',con=hy.con)

a=0
def refresh_database():	
	def refresh():
		global a
		print(a)
		a=a+1

		hy = HuoYan_monitoring(configfile=parser.config)
		hy.collect_infos()

		global df
		df=pd.read_sql('select * from test_lifetime',con=hy.con)

	scheduler = BlockingScheduler()
	scheduler.add_job(refresh, 'interval', seconds=600, id='refresh_database')
	scheduler.start()




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

	dcc.Tabs(id='tabs', children=[
		dcc.Tab(label='Samples in test', children=[
			html.Div(children=[
				html.Div([# 在检样本详情					
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
						columns=[{"name": i, "id": i} for i in df.columns]
					),
					html.Div(id='download_sample',
						style={'display': 'inline-block', 'margin-right': '60px'},
					),
				]),
			]),
		]),
		dcc.Tab(label='Utilities', children=[
			html.Div(children=[
				html.Div(children=[ # utility
					html.H3('qPCR parser'),
					dcc.Upload(id='upload_qPCR',children=[
						html.Div(children=[
							'Drag and Drop or ', html.A('Select Files'),
						],
						style={
							'width': '100%',
							'height': '60px',
							'lineHeight': '60px',
							'borderWidth': '1px',
							'borderStyle': 'dashed',
							'borderRadius': '5px',
							'textAlign': 'center',
							'margin': '10px'},	
						),
					]),
					html.Div(id='download_qPCR',style={'display': 'inline-block', 'margin-right': '60px'},),# download
					html.Hr(),
				]),
				html.Div(children=[
					html.H3('信息录入审核'),
					dcc.Upload(id='upload_info',children=[
						html.Div(children=[
							'Drag and Drop or ', html.A('Select Files'),
						],
						style={
							'width': '100%',
							'height': '60px',
							'lineHeight': '60px',
							'borderWidth': '1px',
							'borderStyle': 'dashed',
							'borderRadius': '5px',
							'textAlign': 'center',
							'margin': '10px'},	
						),
					]),
					html.Div(id='validate_result'),# result
					html.Hr(),
				]),# utility
			]),
		]),

	]),

	
	html.Footer('CopyrightⒸ BGI 2020 版权所有 深圳华大基因股份有限公司 all rights reserved. '),
	
])


# uplod sample info validate
@app.callback(
	dash.dependencies.Output('validate_result','children'),
	[dash.dependencies.Input('upload_info','contents'),
	dash.dependencies.Input('upload_info','filename')]
)
def process_info(contents,filename):
	global parser
	global config
	#return contents
	try:
		_, content_string = contents.split(';') #why 第一次拆不出来
		#print(content_string[:100])
		content_string=content_string.split(',')[1]
		decoded = base64.b64decode(content_string)

		#print(decoded)

		ndf=pd.read_csv(io.StringIO(decoded.decode('utf-8')))

		log_stream = validate(ndf,filename)  

		return "Successfully, Please update to MYBGI"
	except Exception as e:
		return str(e)

# upload qPCR and withdraw the paser result
@app.callback(
	dash.dependencies.Output('download_qPCR','children'),
	[dash.dependencies.Input('upload_qPCR','contents'),
	dash.dependencies.Input('upload_qPCR','filename')]
)
def process_qPCR(contents,filename):
	global parser
	global config
	#return contents
	try:
		_, content_string = contents.split(';') #why 第一次拆不出来
		#print(content_string[:100])
		content_string=content_string.split(',')[1]
		decoded = base64.b64decode(content_string)

		ndf=qPCR_parser([io.StringIO(decoded.decode('utf-8')),],
			[filename,],
			db=parser.db,
			qPCR_Result=config['qPCR_Result'],
			paibandan=config['paibandan'], 
			paibandan_RQ=config['paibandan_RQ'])
		csv_string = ndf.to_csv(index=False, encoding='utf-8')
		csv_string = "data:text/csv;charset=gb2312,\ufeff" + urllib.parse.quote(csv_string)
		basename=filename[0].split('.')[0]
		filename = f'{basename}_parsed.csv'  

		return html.A(
					'Download',
					id='download_parsed_qPCR_link',
					download=filename,
					href=csv_string,				
					target="_blank",
				)
	except Exception as e:
		return str(e)
		
	

# download_sample_link
@app.callback(
	dash.dependencies.Output('download_sample','children'),
	[dash.dependencies.Input('category','value')]
)
def update_download_sample_link(cate_value):
	global df
	if cate_value == 'no_info':
		ndf=df[df.test.notnull() & df['sample'].isnull() & df.finished.isnull()]
	elif cate_value == 'no_sample':
		ndf=df[df.finished.isnull() & df['sample'].notnull() & df['test'].isnull() & df['exception'].isnull()]
	elif cate_value == 'no_report':
		ndf=df[df.finished.isnull() & df['sample'].notnull() & df.test.notnull() & df.report.isnull()]
	else:
		raise ValueError('不支持的类别',cate_value)

	csv_string = ndf.to_csv(index=False, encoding='utf-8')
	csv_string = "data:text/csv;charset=gb2312,\ufeff" + urllib.parse.quote(csv_string)
	filename = f'{cate_value}.csv'  

	return html.A(
				'Download',
				id='download_sample_link',
				download=filename,
				href=csv_string,				
				target="_blank",
			)

# statistics
@app.callback(
	dash.dependencies.Output('statistics','children'),
	[dash.dependencies.Input('submit-button', 'n_clicks')]
)
def get_statistics(n_clicks):
	today_test_df=df[df['test'].str.contains(today).fillna(False)] # 今天到样
	today_report_df=df[df['report'].str.contains(today).fillna(False)] # 今天报告

	statistics = f'''
	当日到样：{len(today_test_df)}\t当日已发报告：{len(today_report_df)}\n\n


	累计到样：{len(df)}\t累计发送报告：{len(df[df['report'].notnull()])}\t累计异常结束：{len(df[df['finished'].notnull() & df['exception'].notnull()])}\t检测中：\t{len(df[df['finished'].isnull()])}\n

	'''
	return statistics

# table data
@app.callback(
	dash.dependencies.Output('table','data'),
	[dash.dependencies.Input('submit-button', 'n_clicks'),
	dash.dependencies.Input('category','value'),
	dash.dependencies.Input('statistics','children')]
)
def get_table_data(n_clicks,cate_value,state):
	global df
	if cate_value == 'no_info':
		ndf=df[df.test.notnull() & df['sample'].isnull() & df.finished.isnull()]
	elif cate_value == 'no_sample':
		ndf=df[df.finished.isnull() & df['sample'].notnull() & df['test'].isnull() & df['exception'].isnull()]
	elif cate_value == 'no_report':
		ndf=df[df.finished.isnull() & df['sample'].notnull() & df.test.notnull() & df.report.isnull()]
	else:
		raise ValueError('不支持的类别',cate_value)

	return ndf.to_dict("rows")

if __name__ == '__main__':
	thread_refresh = threading.Thread(target=refresh_database)
	thread_refresh.start()

	app.run_server(debug=True,port=8050)
	# for production environment, debug must be False
	#app.run_server(debug=False,port=8080)