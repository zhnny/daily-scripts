#!/usr/bin/env python
# coding: utf-8

import requests
import time
import json
import pymysql

def crawler():
   response = requests.get(url)
   # 序列化为dict
   print(response.text)
   data = response.json()

   # dict转JSON
   # data = json.dumps(response.json())
   print(data)

   length = len(data["list"])
   print(length)

   # import pymysql

   # # 打开数据库连接
   # db = pymysql.connect(host="localhost",user="weather_admin",password="weather_admin",database="weather" )

   # # 使用 cursor() 方法创建一个游标对象 cursor
   # cursor = db.cursor()

   # # 使用 execute()  方法执行 SQL 查询 
   # cursor.execute("SELECT VERSION()")

   # # 使用 fetchone() 方法获取单条数据.
   # data = cursor.fetchone()

   # print ("Database version : %s " % data)

   # # 关闭数据库连接
   # db.close()


   # 打开数据库连接
   db = pymysql.connect(host="localhost",user="weather_admin",password="weather_admin",database="weather" )



   # 使用 cursor() 方法创建一个游标对象 cursor
   cursor = db.cursor()


   date = data["dateTime"][:8]
   print(date)


   str_list = list(date)
   str_list.insert(4, " ")
   str_list.insert(7, " ")
   result = ''.join(str_list)
   print(result)


   
   # 将格式字符串转换为时间戳

   print(time.strptime(result,"%Y %m %d"))


   date = time.strftime("%Y-%m-%d", time.strptime(result,"%Y %m %d"))
   print(date)


   # 1、date类型
   # date = datetime.datetime.now.strftime("%Y-%m-%d")
   # sql = “INSERT INTO TABLE_NAME(字段) values(str_to_date(’%s’,’%%Y-%%m-%%d’))”%(date)


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
      print(i)


   # for i in range(length):
   #    city = data["list"][i]["m"]
   #    province = data["list"][i]["s"]
   #    latitude = data["list"][i]["j"]
   #    longitude = data["list"][i]["w"]
   #    sql = "insert into province_city (city,province,latitude,longitude)  values('%s','%s','%f','%f')" %  (city,province,latitude,longitude)
   #    try:
   #       # 执行sql语句
   #       cursor.execute(sql)
   #       # 提交到数据库执行
   #       db.commit()
   #    except:
   #       # 如果发生错误则回滚
   #       db.rollback()
   #    print(i)



   # 关闭数据库连接
   db.close()


url = "http://data.cma.cn/dataGis/exhibitionData/getMarker?dateTime=20210616200000&funitemmenuid=115990101&province=1000&typeCode=NWST"
pressure = "http://data.cma.cn/dataGis/exhibitionData/getMarker?dateTime=20210529200000&funitemmenuid=115990102&province=1000&typeCode=NWST"


for i in range(6):
   days = [31,28,31,30,31,15]
   url_1 = "http://data.cma.cn/dataGis/exhibitionData/getMarker?dateTime=2021"
   url_2 = "200000&funitemmenuid=115990101&province=1000&typeCode=NWST"
   for j in range(days[i]):
      if(j<10):
         url = url_1+"0"+str(i+1)+"0"+str(j+1)+url_2
      else:
         url = url_1+"0"+str(i+1)+str(j+1)+url_2
      print(url)
      crawler()
      time.sleep(30)




