#!/usr/bin/env python
# coding: utf-8

import requests
import pymysql
import time


def crawler(url_list):
   # 打开数据库连接
   # db = pymysql.connect(host="localhost",user="weather_admin",password="weather_admin",database="weather" )
   # db = pymysql.connect(host="db4free.net",user="zhnny_admin",password="zhnny_admin",database="zhnnydb" )
   db = pymysql.connect(host="120.25.253.69",user="weather_admin",password="weather_admin",database="weather_admin" )
   # 使用 cursor() 方法创建一个游标对象 cursor
   cursor = db.cursor()
   for n in range(len(url_list)):
      url=url_list[n]
      response = requests.get(url)
      response.text
      data = response.json()
      length = len(data["list"])
      date = data["dateTime"][:8]
      str_list = list(date)
      str_list.insert(4, " ")
      str_list.insert(7, " ")
      result = ''.join(str_list)
      # 将格式字符串转换为时间戳
      date = time.strftime("%Y-%m-%d", time.strptime(result,"%Y %m %d"))
      if(n==0):
         for i in range(length):
            city = data["list"][i]["m"]
            temperature = data["list"][i]["t"]
            date = date 
            sql = "insert into temperature (city,date,temperature)  values('%s','%s','%s')" %  (city,date,temperature)
            try:
               # 执行sql语句
               cursor.execute(sql)
               # 提交到数据库执行
               db.commit()
            except:
               # 如果发生错误则回滚
               db.rollback()
      if(n==1):
         for i in range(length):
            city = data["list"][i]["m"]
            humidness = data["list"][i]["r"]
            date = date 
            sql = "insert into humidness (city,date,humidness)  values('%s','%s','%f')" %  (city,date,humidness)
            try:
               # 执行sql语句
               cursor.execute(sql)
               # 提交到数据库执行
               db.commit()
            except:
               # 如果发生错误则回滚
               db.rollback()
      if(n==2):
         for i in range(length):
            city = data["list"][i]["m"]
            rainfull = data["list"][i]["r1"]
            date = date 
            sql = "insert into rainfull (city,date,rainfull)  values('%s','%s','%f')" %  (city,date,rainfull)
            try:
               # 执行sql语句
               cursor.execute(sql)
               # 提交到数据库执行
               db.commit()
            except:
               # 如果发生错误则回滚
               db.rollback()
      if(n==3):
         for i in range(length):
            city = data["list"][i]["m"]
            pressure = data["list"][i]["p"]
            date = date 
            sql = "insert into pressure (city,date,pressure)  values('%s','%s','%s')" %  (city,date,pressure)
            try:
               # 执行sql语句
               cursor.execute(sql)
               # 提交到数据库执行
               db.commit()
            except:
               # 如果发生错误则回滚
               db.rollback()
   # 关闭数据库连接
   db.close()

# temperature="http://data.cma.cn/dataGis/exhibitionData/getMarker?dateTime=20210616200000&funitemmenuid=115990101&province=1000&typeCode=NWST"
# humid="https://data.cma.cn/dataGis/exhibitionData/getMarker?dateTime=20210529200000&funitemmenuid=115990103&province=1000&typeCode=NWST"
# rain= "https://data.cma.cn/dataGis/exhibitionData/getMarker?dateTime=20210529200000&funitemmenuid=1150101020&province=1000&typeCode=NWST"
# pressure="http://data.cma.cn/dataGis/exhibitionData/getMarker?dateTime=20210616200000&funitemmenuid=115990102&province=1000&typeCode=NWST"
for i in range(8,9):
   # days = [31,28,31,30,31,30,31,31,16]
   days = [27]
   url_1 = "http://data.cma.cn/dataGis/exhibitionData/getMarker?dateTime=2021"
   url_2 = "&province=1000&typeCode=NWST"
   url_temperature="200000&funitemmenuid=115990101"
   url_humid="200000&funitemmenuid=115990103"
   url_rain="200000&funitemmenuid=1150101020"
   url_pressure="200000&funitemmenuid=115990102"
   for j in range(15,days[0]):
      if(j<10):
         temperature=url_1+"0"+str(i+1)+"0"+str(j+1)+url_temperature+url_2
         humid=url_1+"0"+str(i+1)+"0"+str(j+1)+url_humid+url_2
         rain=url_1+"0"+str(i+1)+"0"+str(j+1)+url_rain+url_2
         pressure=url_1+"0"+str(i+1)+"0"+str(j+1)+url_pressure+url_2
      else:
         temperature=url_1+"0"+str(i+1)+str(j+1)+url_temperature+url_2
         humid=url_1+"0"+str(i+1)+str(j+1)+url_humid+url_2
         rain=url_1+"0"+str(i+1)+str(j+1)+url_rain+url_2
         pressure=url_1+"0"+str(i+1)+str(j+1)+url_pressure+url_2
      url_list=[temperature,humid,rain,pressure]
      crawler(url_list=url_list)
      print("已完成 第 " + str(i) + " 月 " + str(j) + "日")
      time.sleep(30)

