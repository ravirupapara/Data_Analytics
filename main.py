import os
import re
import json
import numpy as np
import pandas as pd
import configparser
from snowflake import connect
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from datetime import date,datetime
from pyspark.sql import SparkSession

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


config=configparser.ConfigParser()

ROOT_DIR=os.path.dirname(os.path.abspath(__file__))
head,_=os.path.spliit(ROOT_DIR)
config.read(head+"\\"+"credentials.ini")

#Kafka execution parameters
input_file=config['kafka']['input_file']
kafka_broker=config['kafka']['kafka_broker']
topic_name=config['kafka']['topic_name']
output_kafka=config['kafka']['output_path']

#Snowflake execution parameters
user=config['snowflake']['user_name']
password=config['snowflake']['password']
account=config['snowflake']['account']
warehouse=config['snowflake']['warehouse']
database=config['snowflake']['database']
schema=config['snowflake']['schema']

#Reading all dependent files 

exec(open('Extraction_from_snowflake.py')).read()
exec(open('Batch_data_using_kafka.py')).read()
exec(open('load_product.py')).read()
exec(open('info.py')).read()




#Calling function kafka_batch_processing from program Batch_data_using_kafka

kafka_batch_processing(kafka_broker,topic_name,output_kafka)

#Calling function snowflake_data_extraction from program Extraction_from_snowflake 

df=snowflake_data_extraction(user,password,account,warehouse,database,schema)

#Calling function load_product_pyspark from load_product.py

product_SKU_pandas,product_pandas=load_product_pyspark(input_file)

#Calling function info from program info.py
info=info(df)

display(info)


Purchased_Store_df=df.groupby(['STORE_NAME','STORE_REGION','PURCHASED_AMOUNT($)'])['PURCHASED_AMOUNT($)'].sum()
display(Purchased_Store_df)

customers=df[df['PURCHASE_DATE']>='01/01/23']

x_data=['STORE_REGION']
y_data=['PURCHASED_AMOUNT($)']


#Creating bar plot to visualize customer purchase region wise
plt.figure(figsize=(10,6))
plt.scatter(x_data,y_data,color='blue',label='Data Points',s=80,alpha=0.7)
plt.xlabel('USERS',fontsize=12)
plt.ylabel('AMOUNT',fontsize=12)
plt.title('Year 2023 Purchase Data Region wise',fontsize=16)
plt.legend(fontsize=10)
plt.grid(True)
plt.tight_layout()
plt.show()

plt.xticks(rotation=15)
plt.yticks(fontsize=10)

# Add text annotations for selected data points
for i, txt in enumerate(df['ADD_INFO']):
    plt.annotate(txt, (x_data[i], y_data[i]), fontsize=8, ha='right')

# Add a trendline (fitting a linear regression)
z = np.polyfit(x_data, y_data, 1)
p = np.poly1d(z)
plt.plot(x_data, p(x_data), color='red', linestyle='--', label='Trendline')

# Show the plot with added features
plt.legend()
plt.tight_layout()
plt.show()



 

