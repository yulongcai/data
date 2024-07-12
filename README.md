本代码给予local实现  
安装相应的文件：python、 pyspark、panda、airflow  

1、搭建airflow  
修改airflow.confg   
  dags_folder ： 当前dags  
  
2、修改文件地址  
  将代码中的文件地址修改成自己的目录

流程：  
download数据存到本地 ——> 加载本地数据process并输出csv文件到本地 ——> 加载csv文件并publish成excel文件
