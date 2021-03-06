#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import re
import numpy as np
import pandas as pd
from collections import defaultdict
import json
import sqlite3
import logging
import time
import yaml
import smtplib
import mimetypes
from email.message import EmailMessage
import datetime
from imbox import Imbox
import email.header
import shutil
import zipfile


class HuoYan_monitoring(object):
    def __init__(
        self, dbname: str = "HuoYan_records.db", configfile: str = "config.yaml"
    ):

        if not os.path.exists(dbname):
            self.con = sqlite3.connect(dbname)
            self.creat_table()
        else:
            self.con = sqlite3.connect(dbname)

        if os.path.exists(configfile):
            configfile = os.path.abspath(configfile)
        else:
            raise ValueError("无效的config文件", configfile)

        with open(configfile, "r", encoding="utf-8") as f:
            self.config = yaml.load(f.read(), Loader=yaml.FullLoader)

    def creat_table(self):
        """
        test_lifetime:
            id: 样品编号
            name:   姓名
            organization:   组织名称
            sample: 样本信息到达的时间
            test:   灭活组确认进入检测的时间
            extract:    提取组提取时间
            board_index:  PCR版号
            hole_index: PCR孔位
            report: 报告时间
            exception:异常情况
            finnished:样本生命周期是否结束

        processed_sample_files:
            sample_file:    文件名称，绝对路径
            modify_time:    最后修改时间

        processed_test_files:
            sample_file:    文件名称，绝对路径
            modify_time:    最后修改时间

        processed_report_files:
            sample_file:    文件名称，绝对路径
            modify_time:    最后修改时间

        processed_mail:
            sample_file:    文件名称，绝对路径
            modify_time:    最后修改时间
        """
        self.con.execute(
            "create table test_lifetime(id,name,organization,unit,board_index,hole_index,sample,test,extract,report,exception,finished)"
        )
        self.con.execute(
            "create table sample_info(product_id,khbm,sjdw,id,sample_type,sample_date,name,sex,age,zjhm,phone)"
        )
        self.con.execute("create table processed_sample_files(sample_file,modify_time)")
        self.con.execute("create table processed_test_files(test_file,modify_time)")
        self.con.execute(
            "create table processed_extract_files(extract_file,modify_time)"
        )
        self.con.execute("create table processed_report_files(report_file,modify_time)")
        self.con.execute("create table processed_mail(mail_uid)")
        self.dbcommit()

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    def fetch_mail(
        self,
        imap_server: str = "mail.genomics.cn",
        port: int = 143,
        start_time: datetime.datetime = datetime.datetime.fromtimestamp(
            time.mktime(datetime.datetime.now().date().timetuple())
        ),
    ):

        user = self.config["mail_user"]
        passwd = self.config["mail_passwd"]
        out_path = self.config["report_dir"]

        imbox = Imbox(
            imap_server, username=user, password=passwd, ssl=False, starttls=False
        )
        messages = imbox.messages(date__gt=start_time)

        for _, message in messages:
            subject = message.subject
            # if '新冠报告' in subject:
            if re.findall(r"新.+冠.+报.+告", subject):
                for attachment in message.attachments:
                    name, _ = email.header.decode_header(attachment["filename"])[0]
                    # if '报告' in name  and 'zip' in name:
                    if re.findall(r"报.+告", name) and re.findall(r"z.+i.+p", name):
                        name = re.sub(r"\?\=\ \=\?UTF\-8\?Q\?", "", name)

                        res = self.con.execute(
                            rf"select * from processed_mail where mail_uid = '{name}'"
                        ).fetchone()
                        if res != None:  # 如果已处理该邮件，则不再收取
                            continue

                        # filename=re.sub(r'\?\=\ \=\?UTF\-8\?Q\?','',filename)#why
                        filename = os.path.join(out_path, "tmp", name)
                        content = attachment["content"]

                        with open(filename, "wb") as w_f:
                            w_f.write(content.getvalue())

                        self.con.execute(
                            f"insert into processed_mail(mail_uid) values ('{name}')"
                        )
                        self.dbcommit()

                        # 在该位置处理解压缩和文件归档
                        target = filename.split(".")[0]
                        if os.path.exists(target):
                            continue

                        myzip = zipfile.ZipFile(filename)
                        myzip.extractall(target)

                        bgmx_path = os.path.join(out_path, "报告明细表")
                        bgcd_path = os.path.join(out_path, "报告存档")

                        _, uuid = re.findall(r"-(.+?)(\d+)", os.path.basename(target))[
                            0
                        ]
                        for sub in os.listdir(target):
                            sub_path = os.path.join(target, sub)

                            bgmx = os.path.join(sub_path, "报告清单明细.xlsx")
                            bgmx_target = os.path.join(
                                bgmx_path, f"{uuid[0:8]}_{uuid[8:]}_报告清单明细.xlsx"
                            )
                            shutil.copy(bgmx, bgmx_target)

                            for sub_sub in os.listdir(sub_path):
                                sub_sub_full = os.path.join(sub_path, sub_sub)
                                if os.path.isdir(sub_sub_full):
                                    bg_path = sub_sub_full
                                    for bg_file in os.listdir(bg_path):
                                        if bg_file.startswith("~"):
                                            continue

                                        if bg_file.endswith("pdf"):
                                            bg_full = os.path.join(bg_path, bg_file)
                                            bg_target = os.path.join(bgcd_path, bg_file)
                                            shutil.copy(bg_full, bg_target)

    def collect_infos(self):
        """采集信息"""
        # 送样信息
        for file in os.listdir(self.config["sample_dir"]):
            if not file.endswith("xlsx"):
                continue
            if file.startswith("~"):
                continue

            file = os.path.join(self.config["sample_dir"], file)
            try:
                self.sample_info(file)
            except Exception as e:
                print("sample file error", file)
                print(e)

        # 实验信息
        # update corresponding to the subfolder change
        dir = self.config["test_dir"]
        for sub in os.listdir(dir):
            fsub = os.path.join(dir, sub)

            if os.path.isdir(fsub):
                for file in os.listdir(fsub):
                    if not file.startswith("20"):
                        continue
                    if "出库样本明细表" not in file:
                        continue
                    if file.startswith("~"):
                        continue

                    ffile = os.path.join(fsub, file)
                    try:
                        self.test_info(ffile)
                    except Exception as e:
                        print("test file error", file)
                        print(e)

        # 提取信息
        dir = self.config["extract_dir"]
        for sub in os.listdir(dir):
            fsub = os.path.join(dir, sub)

            if os.path.isdir(fsub):
                for file in os.listdir(fsub):
                    if file.startswith("~"):
                        continue
                    if not file.endswith("xlsx"):
                        continue

                    ffile = os.path.join(fsub, file)
                    try:
                        self.extract_info(ffile)
                    except Exception as e:
                        print("extract file error", file)
                        print(e)

        # 报告信息

        report_dir = os.path.join(self.config["report_dir"])
        for file in os.listdir(report_dir):
            if not file.endswith("结果反馈表.xlsx"):
                continue
            if file.startswith("~"):
                continue

            file = os.path.join(report_dir, file)
            try:
                self.report_info(file)
            except Exception as e:
                print("report file error", file)
                print(e)

        # 异常信息
        try:
            self.exception_info(self.config["exception_file"])
        except Exception as e:
            print("exception file error", file)
            print(e)

    def file_modify_time(self, file: str):
        """文件的最后修改时间"""
        mtime = os.stat(file).st_mtime
        file_modify_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mtime))

        return file_modify_time

    def sample_info(self, file: str):
        """ sample 表处理"""
        file = os.path.abspath(file)
        res = self.con.execute(
            f"select modify_time from processed_sample_files where sample_file = '{file}'"
        ).fetchone()
        # 如果不存在记录或者记录的时间早于当前文件修改时间，则进行更新
        if res == None or self.file_modify_time(file) > res[0]:
            df = pd.read_excel(file, parse_dates=["样品采集日期"], mode="r")  # 从表格内部处理日期
            # 更新lifetime 表格
            for id, name, unit, organization, date in zip(
                df["样品编号"], df["姓名"], df["科室"], df["送检单位"], df["样品采集日期"]
            ):
                resn = self.con.execute(
                    f"select * from test_lifetime where id ='{id}'"
                ).fetchone()
                if resn == None:  # 所有表中均未出现过
                    self.con.execute(
                        f"insert into test_lifetime(id,unit,name,organization,sample) values ('{id}','{unit}','{name}','{organization}','{date}')"
                    )
                else:  # test表中先出现了该编号
                    self.con.execute(
                        f"update test_lifetime set name='{name}',unit='{unit}',organization='{organization}',sample='{date}' where id='{id}'"
                    )

            # 更新新的样本信息表格
            for (
                product_id,
                khbm,
                sjdw,
                id,
                sample_type,
                sample_date,
                name,
                sex,
                age,
                zjhm,
                phone,
            ) in zip(
                df["产品编号"],
                df["客户编码"],
                df["送检单位"],
                df["样品编号"],
                df["样品类型"],
                df["样品采集日期"],
                df["姓名"],
                df["性别"],
                df["年龄"],
                df["证件号码"],
                df["电话"],
            ):
                resn = self.con.execute(
                    f"select * from sample_info where id ='{id}'"
                ).fetchone()
                # print(product_id,khbm,sjdw,id,sample_type,sample_date,name,sex,age,zjhm,phone)
                # print(f"insert into sample_info(product_id,khbm,sjdw,id,sample_type,sample_date,name,sex,age,zjhm,phone) values ('{product_id}','{khbm}','{sjdw}','{id}','{sample_type}','{sample_date}','{name}','{sex}','{age}','{zjhm}','{phone}')")
                # print(f"update sample_info set product_id='{product_id}',khbm='{khbm}',sjdw='{sjdw}',id='{id}',sample_type='{sample_type}',sample_date='{sample_date}',name='{name}',sex='{sex}',age='{age}',zjhm='{zjhm}',phone='{phone}'  where id='{id}'")
                zjhm = re.sub("'", "", str(zjhm))
                if resn == None:  # 所有表中均未出现过
                    self.con.execute(
                        f"insert into sample_info(product_id,khbm,sjdw,id,sample_type,sample_date,name,sex,age,zjhm,phone) values ('{product_id}','{khbm}','{sjdw}','{id}','{sample_type}','{sample_date}','{name}','{sex}','{age}','{zjhm}','{phone}')"
                    )
                else:
                    self.con.execute(
                        f"update sample_info set product_id='{product_id}',khbm='{khbm}',sjdw='{sjdw}',id='{id}',sample_type='{sample_type}',sample_date='{sample_date}',name='{name}',sex='{sex}',age='{age}',zjhm='{zjhm}',phone='{phone}'  where id='{id}'"
                    )

        # 这里需要检查res的当前状态，没有的话是新建，有的话需要改成更新，不优雅
        if res == None:
            self.con.execute(
                f"insert into processed_sample_files(sample_file,modify_time) values ('{file}','{self.file_modify_time(file)}')"
            )
        else:
            self.con.execute(
                f"update processed_sample_files set modify_time='{self.file_modify_time(file)}' where sample_file = '{file}'"
            )
        self.dbcommit()

    def test_info(self, file: str):
        file = os.path.abspath(file)
        res = self.con.execute(
            f"select modify_time from processed_test_files where test_file = '{file}'"
        ).fetchone()
        if res == None or self.file_modify_time(file) > res[0]:  # 表格未处理，处理表格
            df = pd.read_excel(file, sheet_name="出库样本明细", mode="r")

            ids = []
            schools = []
            for col in df.columns:
                for id in df[col]:
                    if str(id).startswith("20S") and len(str(id)) == 10:
                        ids.append(id)
                        schools.append(col)

            date = os.path.basename(file).split("_")[0]  # 从文件名称中提取日期
            if len(date) != 8:
                raise ValueError("检测表格错误的日期格式", file)

            ndf = pd.DataFrame(
                {"id": ids, "unit": schools, "date": [date for i in range(len(ids))]}
            )
            ndf.date = pd.to_datetime(ndf.date, format="%Y%m%d")

            for id, unit, date in zip(ndf["id"], ndf["unit"], ndf["date"]):
                resn = self.con.execute(
                    f"select * from test_lifetime where id ='{id}'"
                ).fetchone()
                if resn == None:  # 所有表中均未出现过，保留
                    self.con.execute(
                        f"insert into test_lifetime (id,unit,test) values ('{id}','{unit}','{date}')"
                    )
                else:  # sample表中先出现了该编号
                    self.con.execute(
                        f"update test_lifetime set test='{date}' where id='{id}'"
                    )
        if res == None:
            self.con.execute(
                f"insert into processed_test_files(test_file,modify_time) values ('{file}','{self.file_modify_time(file)}')"
            )
        else:
            self.con.execute(
                f"update processed_test_files set modify_time='{self.file_modify_time(file)}' where test_file = '{file}'"
            )

        self.dbcommit()

    def extract_info(self, file):
        file = os.path.abspath(file)
        res = self.con.execute(
            f"select modify_time from processed_extract_files where extract_file = '{file}'"
        ).fetchone()
        if res == None or self.file_modify_time(file) > res[0]:  # 表格未处理，处理表格

            date = os.path.basename(file).split("-")[0]  # 从文件名称中提取日期
            board_index = os.path.basename(file).split(".")[0]  # 从文件名称中提取板号
            if len(date) != 8:
                raise ValueError("检测表格错误的日期格式", file)

            df = pd.read_excel(file, mode="r")
            df = df.iloc[:, [list(df.columns).index('样例编号')-1,list(df.columns).index('样例编号')]] #位置发生了变化
            #df = df.rename(columns={"Unnamed: 12": "hole_index", "样例编号": "id"})
            df.columns=pd.Index(['hole_index','id'])
            df["date"] = date
            df.date = pd.to_datetime(df.date, format="%Y%m%d")
            df["board_index"] = board_index
            df = df[df.id.notnull()]

            for id, date, board_index, hole_index in zip(
                df["id"], df["date"], df["board_index"], df["hole_index"]
            ):
                id=str(id).upper()
                if len(str(id)) != 10:  # filter illegal sample ids
                    continue

                resn = self.con.execute(
                    f"select * from test_lifetime where id ='{id}'"
                ).fetchone()
                if resn == None:  # 编号不存在时也针对提取新建记录                    
                    self.con.execute(
                        f"insert into test_lifetime(id,extract,board_index,hole_index) values ('{id}','{date}','{board_index}','{hole_index}')"
                    )
                else:
                    self.con.execute(
                        f"update test_lifetime set extract='{date}', board_index='{board_index}', hole_index='{hole_index}' where id='{id}'"
                    )

        self.con.execute(
            f"insert into processed_extract_files(extract_file,modify_time) values ('{file}','{self.file_modify_time(file)}')"
        )

        self.dbcommit()

    def report_info(self, file):  # 重构20200508，
        file = os.path.abspath(file)
        res = self.con.execute(
            f"select modify_time from processed_report_files where report_file = '{file}'"
        ).fetchone()
        if res == None or self.file_modify_time(file) > res[0]:  # 表格未处理，处理表格
            date = os.path.basename(file).split("_")[0]  # 从文件名称中提取日期
            if len(date) != 8:
                print(file)
                raise ValueError("检测表格错误的日期格式")

            df = pd.read_excel(file, sheet_name="阴性", mode="r")
            df = df.rename(columns={"结果": "result", "样本名称": "id"})
            df["date"] = date
            df.date = pd.to_datetime(df.date, format="%Y%m%d")
            df = df[df.id.notnull()]

            for id, result, date in zip(df["id"], df["result"], df["date"]):
                if result == "阴性":  # 判断结果是阴性还是检测失败
                    self.con.execute(
                        f"update test_lifetime set report='{date}',finished=1 where id='{id}'"
                    )
                else:
                    self.con.execute(
                        f"update test_lifetime set exception='{result}',finished=1 where id='{id}'"
                    )

        self.con.execute(
            f"insert into processed_report_files(report_file,modify_time) values ('{file}','{self.file_modify_time(file)}')"
        )
        self.dbcommit()

    def exception_info(self, file):
        df = pd.read_excel(file, skiprows=[0, 1], parse_dates=["送样日期"], mode="r")
        for id, date, hege, qksm in zip(
            df["样本编号"], df["送样日期"], df["实物样本是否合格"], df["情况说明"]
        ):
            resn = self.con.execute(
                f"select * from test_lifetime where id ='{id}'"
            ).fetchone()
            if resn == None:  # 不存在样本，插入操作
                if hege == "是":  # 合格 生命周期不结束
                    self.con.execute(
                        f"insert into test_lifetime (id,test,exception) values ('{id}','{date}','{qksm}') "
                    )
                else:  # 不合格，结束生命周期
                    self.con.execute(
                        f"insert into test_lifetime (id,test,exception,finished) values ('{id}','{date}','{qksm}',1) "
                    )
            else:  # 存在样本，更新操作
                if hege == "是":
                    self.con.execute(
                        f"update test_lifetime set exception='{qksm}',test='{date}' where id='{id}'"
                    )
                else:
                    self.con.execute(
                        f"update test_lifetime set exception='{qksm}',test='{date}',finished=1 where id='{id}'"
                    )

        self.dbcommit()

    def monitoring(self):

        pass

    # 生命周期表
    def get_df(self):
        df = pd.read_sql("select * from test_lifetime", con=self.con)

        return df

    # 总记录表
    def get_full_df(self):
        df = pd.read_sql("select * from test_lifetime", con=self.con)
        smpi = pd.read_sql("select * from sample_info", con=self.con)
        full_df = pd.merge(df, smpi, on="id", how="left")

        return full_df

    # 已到样确认的记录表
    def get_tested_df(self, organizations: list = []):
        df = self.get_full_df()
        if len(organizations) == 0:
            df = df[(df["test"].notnull())]
        else:
            df = df[df["organization"].isin(organizations)]
            df = df[(df["test"].notnull())]

        return df

    # 已到样确认但未发报告的记录表
    def get_unreport_df(self, organizations: list = []):
        df = self.get_full_df()
        if len(organizations) == 0:
            df = df[(df["test"].notnull()) & (df["report"].isnull())]  # 有到样，无报告
        else:
            df = df[df["organization"].isin(organizations)]
            df = df[(df["test"].notnull()) & (df["report"].isnull())]  # 有到样，无报告

        return df

    # 教育局样本的数据统计
    def daily_report(
        self,
        day: str = None,
        jyjs: list = ["哈尔滨市双城区教育局", "五常市教育局", "哈尔滨市南岗区教育局", "哈尔滨市香坊区教育局"],
    ):

        full_df = self.get_full_df()
        full_df = full_df.drop_duplicates(subset=['name_x','zjhm'])

        jyj_df = full_df[(full_df.organization.isin(jyjs))]
        jyj_df["school"] = jyj_df.unit.map(lambda x: re.split("_", x)[0])
        jyj_df["school"] = jyj_df["school"].fillna("nan")

        # 每天的三个子df
        if day:  # 指定了日期
            test_df = jyj_df[(jyj_df["test"].str.contains(day).fillna(False))]
            hege_df = jyj_df[
                (jyj_df["test"].str.contains(day).fillna(False))
                & (jyj_df["exception"].isnull())
            ]
            extract_df = jyj_df[(jyj_df["extract"].str.contains(day).fillna(False))]
            report_df = jyj_df[(jyj_df["report"].str.contains(day).fillna(False))]
        else:  # 未指定日期
            test_df = jyj_df[jyj_df["test"].notnull()]
            hege_df = jyj_df[jyj_df["test"].notnull() & jyj_df["exception"].isnull()]
            extract_df = jyj_df[jyj_df["extract"].notnull()]
            report_df = jyj_df[jyj_df["report"].notnull()]

        # 输出一个异常单(异常结束的样本)
        # jyj_df[jyj_df['test'].notnull() & jyj_df['exception'].notnull()
        #       & jyj_df['finished'].notnull()].to_excel('异常样本.xlsx')
        # test_df.to_excel('ttttt.xlsx')

        day_df = pd.DataFrame(
            [
                test_df.groupby(["organization", "school"])["id"].count(),
                hege_df.groupby(["organization", "school"])["id"].count(),
                extract_df.groupby(["organization", "school"])["id"].count(),
                report_df.groupby(["organization", "school"])["id"].count(),
            ]
        ).T

        day_df.columns = pd.Index(["到样数", "合格数", "检测数", "报告数"])
        day_df = day_df.fillna(0)

        return day_df
