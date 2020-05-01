# ExtractPriColumns

从Mysql数据库抽取数据到文件中，格式如下：  

库名 表名 主键字段 主键字段值  

用法:  
java -jar ExtractPriColumns.jar 127.0.0.1:3306 db root root  

结果存放到当前目录下(result.csv)
