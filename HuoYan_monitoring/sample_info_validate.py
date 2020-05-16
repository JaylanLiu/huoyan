import pandas as pd
import logging
import io
import time
import os
import sys
import re

class MaxLevelTrackingHandler(logging.Handler):
    """Handler that does nothing but track the maximum msg level emitted."""
    def __init__(self):
        """Initialize the handler with an attribute to track the level."""
        super(MaxLevelTrackingHandler, self).__init__()
        self.max_level = logging.NOTSET

    def emit(self, record):
        """Update the maximum level with a new record."""
        self.max_level = max(self.max_level, record.levelno)

    def get_exit_status(self):
        """
        Return an exit status for the validator script based on max_level.
        """
        if self.max_level <= logging.INFO:
            return 0
        elif self.max_level == logging.WARNING:
            return 1
        elif self.max_level >= logging.ERROR:
            return 2

def validate(dfi:pd.DataFrame, 
    filename:str,
    khbm_f:str=r'\\192.168.1.2\d\4-信息录入组\khbm.xls',
    sample_dir:str=r'\\192.168.1.2\d\4-信息录入组',
    tymb:str=r'C:\Users\HuoYan-luruzu\OneDrive\python_code\HaerbinHuoYan\信息录入\杨晓琴-新冠通用模板-信息录入导入模板.xlsx'):

    #logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s: %(message)s")

    log_capture_stream = io.StringIO()
    ch = logging.StreamHandler(log_capture_stream)
    ch.setLevel(logging.ERROR)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    sh = MaxLevelTrackingHandler()
    logger.addHandler(sh)

    #khbm & paras
    df=pd.read_excel(khbm_f,mode='r')
    df=df.applymap(lambda x: str(x).strip())
    khbm=dict(df.to_dict('split')['data'])
    paras={'产品编号':'SD0267','样品类型':'S068'}

    dfi=dfi[dfi['样品编号'].notnull()]

    #validate
    split_file_name = filename.split('.')[0].split('-')
    if len(split_file_name) != 4:
        raise NameError('文件命名错误')
        
    hospital_name,date,batch,num=split_file_name

    if len(date) !=8 :
        raise NameError('日期格式不正确，请按8位年月日')

    num=int(num)
    if len(dfi)!= num:
        raise ValueError('样本数不等于记录数')

    if not hospital_name in khbm:
        raise ValueError('客户编码不存在')

    if len(dfi[dfi['样品编号'].duplicated()])>0:
        raise ValueError('本文件中重复的样本编号',dfi[dfi['样品编号'].duplicated()])

    if len(dfi[dfi['样品编号'].isna()])>0:
        raise ValueError('样品编号为空')

    for i,boolstate in  enumerate(dfi['样品编号'].str.startswith('20S')):
        if boolstate == False:
            raise ValueError('非法的样本编号',dfi.iloc[i])

    if len(dfi[dfi['姓名'].isna()])>0:
        raise ValueError('样品编号为空')

    # 历史批次查重
    dfs=[]
    for i, file in enumerate(os.listdir(sample_dir)):
        if not (file.endswith('xls') or file.endswith('xlsx')):
            continue
        dfs.append(pd.read_excel(os.path.join(sample_dir,file)))  
    if len(dfs)>0: 
        dfa=pd.concat(dfs)
        dups = dfi[dfi['样品编号'].isin(dfa['样品编号'])]
        if len(dups)>0:
            raise ValueError('样品编号已在系统中存在',dups)
        exists_sample_num=len(dfa)
    else:
        exists_sample_num=0

    logger.info('通过格式检查')

    # 生成通用模板
    dfi['产品编号']=[paras['产品编号'] for i in range(num)]
    dfi['样品类型']=[paras['样品类型'] for i in range(num)]
    dfi['客户编码']=[khbm[hospital_name] for i in range(num)]
    dfi['样品采集日期']=[date for i in range(num)]
    dfi['送检单位']=[hospital_name for i in range(num)]
    dfi['姓名']=dfi['姓名'].map(lambda x :re.sub(r'\s+','',x))


    target = os.path.join(sample_dir,f'{date}_{hospital_name}_{batch}_{num}.xlsx')

    dfm = pd.read_excel(tymb)
    dft=dfi.reindex(columns= dfm.columns)
    #print(dft)
    dft.to_excel(target,index=None)

    logger.info('生成通用模板并上传共享')
    logger.info(f'样本总数：{exists_sample_num+len(dft)}，其中本单{len(dft)}')

    return log_capture_stream.getvalue()
