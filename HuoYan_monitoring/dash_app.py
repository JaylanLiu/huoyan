import dash
import dash_table
import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
import dash_auth
import base64
import plotly.graph_objects as go

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

today=str(time.strftime("%Y-%m-%d", time.localtime()))

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
try:
	hy.collect_infos()
except Exception as e:
	print(e)
df=pd.read_sql('select * from test_lifetime',con=hy.con)

a=0
def refresh_database():	
	def refresh():
		global a
		print(a)
		a=a+1

		hy = HuoYan_monitoring(configfile=parser.config)
		try:
			hy.collect_infos()
		except Exception as e:
			print(e)

		global df
		df=pd.read_sql('select * from test_lifetime',con=hy.con)

	scheduler = BlockingScheduler()
	scheduler.add_job(refresh, 'interval', seconds=600, id='refresh_database')
	scheduler.start()


def get_day_item_num(df:pd.DataFrame,day:str,item:str,organization:str='total'):
    if organization !='total':
        ndf=df[(df[item].str.contains(day)).fillna(False) & (df['organization']==organization)]
    else:
        ndf=df[(df[item].str.contains(day)).fillna(False)]
    return len(ndf)


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
		dcc.Tab(id ='records',label='Records',children=[
			html.Div(children=[
				html.Div([
					dcc.Dropdown(
						id='organization',
						options=[{'label': 'total', 'value': 'total'},
								{'label': '哈尔滨市道里区教育局', 'value': '哈尔滨市道里区教育局'},
								{'label': '道外区疾病控制中心', 'value': '道外区疾病控制中心'},
								{'label': '哈尔滨市道外区教育局', 'value': '哈尔滨市道外区教育局'},
								{'label': '哈尔滨市平房区中医院', 'value': '哈尔滨市平房区中医院'},
								{'label': '黑龙江华大医学检验有限公司', 'value': '黑龙江华大医学检验有限公司'},
								{'label': '哈尔滨市道外区疾病预防控制中心', 'value': '哈尔滨市道外区疾病预防控制中心'},
								{'label': '道外区大有社区卫生服务中心', 'value': '道外区大有社区卫生服务中心'},
								{'label': '道外区三棵社区卫生服务中心', 'value': '道外区三棵社区卫生服务中心'},
								{'label': '哈尔滨广积德中医医院有限公司', 'value': '哈尔滨广积德中医医院有限公司'},
								{'label': '哈尔滨三芝堂中医门诊部', 'value': '哈尔滨三芝堂中医门诊部'},
								{'label': '哈尔滨航天医院', 'value': '哈尔滨航天医院'},
								{'label': '冀东水泥黑龙江有限公司', 'value': '冀东水泥黑龙江有限公司'},
								{'label': '黑龙江鲁班建设集团有限公司', 'value': '黑龙江鲁班建设集团有限公司'},
								{'label': '国家税务总局哈尔滨市阿城区税务局', 'value': '国家税务总局哈尔滨市阿城区税务局'},
								{'label': '哈尔滨市双城区教育局', 'value': '哈尔滨市双城区教育局'},
								{'label': '五常市教育局', 'value': '五常市教育局'},
								{'label': '哈尔滨市中医医院', 'value': '哈尔滨市中医医院'},
								{'label': '哈尔滨市南岗区教育局', 'value': '哈尔滨市南岗区教育局'},
								{'label': '哈尔滨市香坊区教育局', 'value': '哈尔滨市香坊区教育局'}],
						value='total',
					),
					dcc.Graph(id='present'),
					
				]),
			]),
		]),
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
						columns=[{"name": i, "id": i} for i in df.columns],
						filter_action="native",
						sort_action="native",
						sort_mode="multi",
						page_action="native",
					),
					html.Div(id='table_interactivity_container'),
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



# present
@app.callback(
	dash.dependencies.Output('present','figure'),
	[dash.dependencies.Input('organization','value')]
)
def dropdown_options(org):
	global df
	last_10_days=pd.DatetimeIndex(end=time.strftime("%Y-%m-%d", time.localtime()),freq='d',periods=10)
	fig = go.Figure()
	days=[str(day)[:10] for day in last_10_days]


	for item,tag in zip(['test','extract','report'],['到样数','检测数','报告数']):
		fig.add_trace(go.Bar(
		x=days,
		y=[get_day_item_num(df,day,organization=org,item=item) for day in days ],
		name=tag,
    
	))
	fig.update_layout(barmode='group', xaxis_tickangle=-90)
	
	return fig


# dropdown options for present. why options did not work
'''
@app.callback(
	dash.dependencies.Output('organization','options'),
	[dash.dependencies.Input('category','value'),
	dash.dependencies.Input('submit-button', 'n_clicks')]
)
def dropdown_options(v,n):
	global df
	options=[]
	options.append({'label':'total','value':'total'})
	for org in df.organization.unique():
		option={'label':org,'value':org}
		options.append(option)

	print(options)
	
	return options
''' 



# table enhancement, filtering
@app.callback(
    dash.dependencies.Output('table_interactivity_container', "children"),
    [dash.dependencies.Input('table', "derived_virtual_data"),
     dash.dependencies.Input('table', "derived_virtual_selected_rows"),
	 dash.dependencies.Input('category','value')])
def update_graphs(rows, derived_virtual_selected_rows,cate_value):
    if derived_virtual_selected_rows is None:
        derived_virtual_selected_rows = []
    dff = df if rows is None else pd.DataFrame(rows)
    colors = ['#7FDBFF' if i in derived_virtual_selected_rows else '#0074D9'
              for i in range(len(dff))]

    return [
        dcc.Graph(
            id=column,
            figure={
                "data": [
                    {
                        "x": dff["country"],
                        "y": dff[column],
                        "type": "bar",
                        "marker": {"color": colors},
                    }
                ],
                "layout": {
                    "xaxis": {"automargin": True},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": column}
                    },
                },
            },
        )
		for column in ["pop", "lifeExp", "gdpPercap"] if column in dff
    ]



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

		#print(decoded.decode('utf-8')) #该处正常

		ndf = pd.read_csv(io.StringIO(decoded.decode('utf-8')), dtype={'证件号码':'str','电话':'str'})
		#print(ndf) #该处也正常

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
	当日到样：{len(today_test_df)};\t当日发出报告：{len(today_report_df)};\n\n


	累计到样：{len(df[df['test'].notnull()])};\t累计发送报告：{len(df[df['report'].notnull()])};\t累计异常结束：{len(df[df['finished'].notnull() & df['exception'].notnull()])};\t检测中：\t{len(df[df['finished'].isnull() & df['test'].notnull()])}\n

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

	#app.run_server(debug=True,port=8050)
	# for production environment, debug must be False
	app.run_server(debug=False,port=8080)