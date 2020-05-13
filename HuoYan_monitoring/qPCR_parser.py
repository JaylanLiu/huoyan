import re
import pandas as pd
import numpy as np
import os
import datetime
import sqlite3


#It should be implemented based on  decision tree stratege further for maintainability

def qPCR_parser(xjsj_fs:list,
                    jgfk_f:str,
                    qPCR_Result:str=r'\\192.168.1.2\d\QPCR Result',
                    paibandan:str=r'\\192.168.1.2\d\2-取样组\排版单',
                    paibandan_RQ:str=r'\\192.168.1.2\d\QPCR&报告组\重Q排版单',
                    db:str=r'C:\Users\HuoYan-luruzu\OneDrive\python_code\HaerbinHuoYan\HuoYan_monitoring\HuoYan_monitoring\HuoYan_records.db'
                ):
    tdfs=[]
    for xjsj_f in xjsj_fs:        
        ## 下机文件的时间从共享位置获取同名db的文件
        def find_qPCR(xjsj_f:str):
            dts=[]
            for date_dir in os.listdir(qPCR_Result):
                if date_dir.startswith('2020'):
                    date_dir_full=os.path.join(qPCR_Result,date_dir)
                    for dt_f in os.listdir(date_dir_full):
                        if dt_f.endswith('dt') and not dt_f.startswith('~'):
                            dt_f_full=os.path.join(date_dir_full,dt_f)
                            dts.append(dt_f_full)

            
            return [target for target in dts if os.path.basename(xjsj_f).split('.')[0] in target][0]



        db_file=find_qPCR(xjsj_f)

        mtime = os.stat(db_file).st_mtime
        ## 排版单中获取样本编号和孔位的对应关系，分为是否重Q的两个位置
        def find_tqrwd(xjsj_f:str):
            qPCR_Result= paibandan if not 'RQ' in xjsj_f else paibandan_RQ
 
            dts = []
            for date_dir in os.listdir(qPCR_Result):
                if date_dir.startswith('2020'):
                    date_dir_full=os.path.join(qPCR_Result,date_dir)
                    for dt_f in os.listdir(date_dir_full):
                        if dt_f.endswith('xlsx') and not dt_f.startswith('~'):
                            dt_f_full=os.path.join(date_dir_full,dt_f)
                            dts.append(dt_f_full)

            

            #return [target for target in dts if '-'.join(os.path.basename(xjsj_f).split('.')[0].split('-')[:2]) in target][0]
            return [target for target in dts if '-'.join([seg for seg in os.path.basename(xjsj_f).split('.')[0].split('-') if seg!='RQ'][:2]) in target][0]

        tqrwd_f=find_tqrwd(xjsj_f)

        df=pd.read_excel(tqrwd_f)
        tqr_df=df.iloc[:,12:14]
        tqr_df=tqr_df[tqr_df['Unnamed: 12'].notnull()]


        ## 读取下机数据并处理

        xdf=pd.read_excel(xjsj_f,index_col=False)

        xdf=xdf[['日期', 'ID', '孔位', '样品名称', '标记物', '样品类型', 'Ct', 'Quantity', '测试结果','定量(Ct)', '定量(Copy)', '测试类型', '单位']]

        ndf=xdf.set_index(['日期', 'ID', '孔位', '样品名称', '标记物', '样品类型', 'Quantity', '测试结果','定量(Ct)', '定量(Copy)', '测试类型', '单位']).unstack(['标记物'])

        banhao=xjsj_f.split('.')[0]

        tdf=ndf['Ct'].reset_index()

        tdf['版号']=os.path.basename(banhao)

        tdf=tdf.rename(columns={'孔位':'反应孔','FAM':'FAM Ct值','测试结果':'结果'})

        tdf['样本名称']=tdf.反应孔.map(tqr_df.set_index('Unnamed: 12')['样例编号'])

        tdf['FAM Ct值']=tdf['FAM Ct值'].replace(' - ','NoCt')

        tdf['QPCR下机时间']=str(datetime.datetime.fromtimestamp(mtime))

        tdf=tdf.reindex(columns=['版号','反应孔','样本名称','FAM Ct值','结果','备注','QPCR下机时间','送检单位','VIC'])
        
        
        #样本单例判定逻辑
        
        def vic_paning(vic:float):
            
            if re.findall('-',str(vic)):
                return '质控不合格'
            elif 32<float(vic)<34:
                return '质控临界'
            elif float(vic)>34:
                return '质控不合格'
            else:
                return np.nan
            
            
        tdf['VIC_判定']= tdf['VIC'].map(lambda x: vic_paning(x))
        
        def fam_panding(fam:float):
            if fam == 'NoCt':
                return np.nan
            elif float(fam)>38:
                return '重Q'
            else:
                return '重提+重Q'
            
        tdf['FAM_判定']=tdf['FAM Ct值'].map(lambda x: fam_panding(x))
                
        tdf=tdf[tdf['样本名称'].notnull()]

        con=sqlite3.connect(db)

        def fetch_unit(id:str):
            cur=con.execute(f"select * from test_lifetime where id = '{id}'").fetchone()
            if cur == None:
                return np.nan
            else:
                return cur[2]

        tdf['送检单位']=tdf['样本名称'].map(lambda x: fetch_unit(x))

        con.close()
        
        
        #在此增加质控品的判定逻辑
        def vic_blank_jundge(vic:str):
            if '-' in  str(vic) :
                return np.nan
            elif float(vic)<32:
                return '质控不合格'
            else:
                return np.nan

        tdf.loc[tdf[tdf.样本名称=='空白质控1'].index,'VIC_判定'] = tdf[tdf.样本名称=='空白质控1'].apply(lambda x :vic_blank_jundge(x.VIC),axis=1)

        tdf.loc[tdf[tdf.样本名称=='空白质控2'].index,'VIC_判定'] = tdf[tdf.样本名称=='空白质控2'].apply(lambda x :vic_blank_jundge(x.VIC),axis=1)

        tdf.loc[tdf[tdf.样本名称=='空白质控3'].index,'VIC_判定'] = tdf[tdf.样本名称=='空白质控3'].apply(lambda x :vic_blank_jundge(x.VIC),axis=1)

        # 空白质控1,2,3的VIC组合逻辑

        kbzk1_judge=tdf[tdf.样本名称=='空白质控1']['VIC_判定'].iloc[0]
        kbzk2_judge=tdf[tdf.样本名称=='空白质控2']['VIC_判定'].iloc[0]

        if pd.isnull(kbzk1_judge) and pd.isnull(kbzk2_judge):
            vic = tdf[tdf.样本名称=='空白质控3']['VIC'].iloc[0]
            if '-' in  str(vic) :
                tdf['VIC_整版判定']=np.nan
            elif float(vic)<32:
                tdf['VIC_整版判定']='整版重Q'
            else:
                tdf['VIC_整版判定']=np.nan


        elif pd.notnull(kbzk1_judge) and pd.notnull(kbzk2_judge):
            tdf['VIC_整版判定']='整版重提'
        else:
            tdf['VIC_整版判定']='人工判读'

        # 阳性质控的VIC逻辑

        def vic_posi_jundge(vic:str):
            if '-' in  str(vic) :
                return '质控不合格'
            elif float(vic)>32:
                return '质控不合格'
            else:
                return np.nan

        tdf.loc[tdf[tdf.样本名称=='阳性质控'].index,'VIC_判定'] = tdf[tdf.样本名称=='阳性质控'].apply(lambda x :vic_posi_jundge(x.VIC),axis=1)

        if pd.notnull(tdf.loc[tdf[tdf.样本名称=='阳性质控'].index,'VIC_判定'].iloc[0]):
            tdf['VIC_整版判定']='整版重提'



        # 空白质控 1,2,3的FAM更新逻辑 有阳性就会重做 不需要做检验

        # 阳性质控的FAM逻辑

        def fam_posi_jundge(fam:str):
            if 'NoCt' in  str(fam) :
                return '质控不合格'
            elif float(fam)>32:
                return '质控不合格'
            else:
                return np.nan

        tdf.loc[tdf[tdf.样本名称=='阳性质控'].index,'FAM_判定'] = tdf[tdf.样本名称=='阳性质控'].apply(lambda x :fam_posi_jundge(x['FAM Ct值']),axis=1)

        if pd.notnull(tdf.loc[tdf[tdf.样本名称=='阳性质控'].index,'FAM_判定'].iloc[0]):
            tdf['FAM_整版判定']='整版重提'
        else:
            tdf['FAM_整版判定']=np.nan

        # 判定组合逻辑

        tdf['结果']=np.nan

        def combine_judge(VIC_judge:str, FAM_judge:str, VIC_whole_judge:str, FAM_whole_judge:str):
            if pd.isnull(VIC_whole_judge):
                if pd.isnull(FAM_whole_judge):
                    if pd.isnull(VIC_judge):
                        if pd.isnull(FAM_judge):
                            return '阴性',np.nan
                        elif FAM_judge == '重提+重Q':
                            return np.nan,'样本重提+重Q'
                        elif FAM_judge == '重Q':
                            return np.nan,'样本重Q'
                        else:
                            raise ValueError('不支持的FAM样本逻辑',FAM_judge)

                    elif VIC_judge=='质控不合格':
                        return np.nan, '样本重提'
                    elif VIC_judge=='质控临界':
                        if pd.isnull(FAM_judge):
                            return np.nan,'质控临界，人工判断'
                        elif FAM_judge == '重提+重Q':
                            return np.nan,'重提'
                        elif FAM_judge == '重Q':
                            return np.nan,'重提'
                        else:
                            raise ValueError('不支持的FAM样本逻辑',FAM_judge)
                        pass
                    else:
                        raise ValueError('不支持的VIC样本逻辑',VIC_judge)
                elif FAM_whole_judge == '整版重提':
                    return np.nan, '整版重提'
                else:
                    raise ValueError ('不支持的FAM整版逻辑',FAM_whole_judge)

            elif VIC_whole_judge == '整版重提':
                return np.nan,'整版重提'
            elif VIC_whole_judge == '整版重Q':
                return np.nan,'整版重Q'
            elif VIC_whole_judge =='人工判断':
                if pd.isnull(FAM_whole_judge):
                    if pd.isnull(VIC_judge):
                        if pd.isnull(FAM_judge):
                            return np.nan,'质控VIC临界人工判断'
                        elif FAM_judge == '重提+重Q':
                            return np.nan,'重提'
                        elif FAM_judge == '重Q':
                            return np.nan,'重提'
                        else:
                            raise ValueError('不支持的FAM样本逻辑',FAM_judge)
                        pass
                    elif VIC_judge == '质控不合格':
                        return np.nan,'样本重提'
                    elif VIC_judge == '质控临界':
                        return np.nan,'样本重提'
                    else:
                        raise ValueError('不支持的VIC样本逻辑',VIC_judge)
                    pass
                elif FAM_whole_judge == '整版重提':
                    return np.nan, '整版重提'
                else:
                    raise ValueError('不支持的FAM整版逻辑',FAM_whole_judge)

            else:
                raise ValueError('不支持的VIC整版逻辑',VIC_whole_judge )

        result_and_remark = tdf.apply(lambda x:combine_judge(x['VIC_判定'],x['FAM_判定'],x['VIC_整版判定'],x['FAM_整版判定']),axis=1)

        results=[]
        remarks=[]
        for x,y in result_and_remark:
            results.append(x)
            remarks.append(y)

        tdf['结果'] = results
        tdf['备注'] = remarks 

        # 阳性质控的单独结果判定逻辑 

        positive_control_fam_ct=tdf.loc[tdf[tdf.样本名称=='阳性质控'].index,'FAM Ct值'].iloc[0]
        if positive_control_fam_ct=='NoCt' or float(positive_control_fam_ct)>32:
            tdf.loc[tdf[tdf.样本名称=='阳性质控'].index,'结果']='阴性'
        elif float(positive_control_fam_ct)<=32:
            tdf.loc[tdf[tdf.样本名称=='阳性质控'].index,'结果']='阳性'
        else:
            raise ValueError ('不正常的阳性质控FAM值',positive_control_fam_ct)
        
        
        tdfs.append(tdf)


    # 合并
    tdft=pd.concat(tdfs)

    exc_jgfk=pd.ExcelWriter(jgfk_f)

    tdft[tdft.送检单位.notnull()].to_excel(exc_jgfk,sheet_name='阴性',index=None)

    tdft[tdft.送检单位.isnull()].to_excel(exc_jgfk,sheet_name='未录入',index=None)

    exc_jgfk.close()
    return tdft