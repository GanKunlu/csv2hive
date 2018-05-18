from pyhive import hive
from TCLIService.ttypes import TOperationState
import sys
import csv
import os

def connect_hive(ip):
    cursor = hive.connect(ip).cursor()
    return cursor
          
def get_csv_column(file_path,encode): 
    _reader = csv.reader(open(file_path,encoding = encode))
    return _reader.__next__()
             
file_path = sys.argv[1] 
encode_type  = sys.argv[2]
colunm_type  = sys.argv[3] 
cursor = connect_hive('192.168.23.223')
table_name = os.path.split(file_path)[1][:-4]

if colunm_type == '0': 
    colunm = ["c{0}".format(i) for i in range(len(get_csv_column(file_path,encode = encode_type)))]
else:
    colunm = get_csv_column(file_path, encode = encode_type)
arg_input = [i+' String' for i in colunm]
sql0 = """drop table if exists otemp.temp_load_%(table)s""" % {'table':table_name}
sql1 = """create table otemp.temp_load_%(table)s(%(input)s) row format delimited fields terminated by ',' tblproperties ('skip.header.line.count'='%(line)s')""" % {'table':table_name, 'input':', '.join(arg_input), 'line': colunm_type}
sql2 = """ALTER TABLE otemp.temp_load_%(table)s  SET SERDEPROPERTIES ('serialization.encoding'='%(enco)s')""" % {'table':table_name, 'enco': encode_type}
sql3 = """LOAD DATA LOCAL INPATH '/home/gankunlu/hive/load_csv/%(path)s' INTO TABLE otemp.temp_load_%(table)s""" % {'table':table_name, 'path': file_path}
print(sql1)
print(sql2)
print(sql3)
cursor.execute(sql0)
cursor.execute(sql1)
cursor.execute(sql2)
#cursor.execute(sql3)
os.system("""hive -e "%s;" """%(sql3))
print("load %s was done!" %(file_path))
print("table name:")
print('otemp.temp_load_'+table_name)
print("column is:")
print(", ".join(colunm))



