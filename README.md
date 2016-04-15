# HiveKudu-Handler
Hive Kudu Storage Handler, Input & Output format, Writable and SerDe

This is the first release of Hive on Kudu.

I have placed the jars in the Resource folder which you can add in hive and test.

If you would like to build from source then make install and use "HiveKudu-Handler-0.0.1.jar" to add in hive cli or hiveserver2 lib path.

## Working Test case
### simple_test.sql
```sql
add jar HiveKudu-Handler-0.0.1.jar; 
add jar kudu-client-0.6.0.jar; 
add jar async-1.3.1.jar;

set hive.cli.print.header=true;

CREATE TABLE if not exists test_drop (
id INT,
name STRING
)
stored by 'org.apache.hadoop.hive.kududb.KuduHandler.KuduStorageHandler'
TBLPROPERTIES(
  'kudu.table_name' = 'test_drop',
  'kudu.master_addresses' = 'ip-172-31-56-74.ec2.internal:7051',
  'kudu.key_columns' = 'id'
);

describe formatted test_drop;

insert into test_drop values (1, 'a'), (2, 'b'), (3, 'a');

select count(*) from test_drop;

select id from test_Drop where name = 'a';

select name, count(*) from test_drop group by name;

drop table test_Drop;
```

### Output of simple test
```

Logging initialized using configuration in jar:file:/opt/cloudera/parcels/CDH-5.5.2-1.cdh5.5.2.p0.4/jars/hive-common-1.1.0-cdh5.5.2.jar!/hive-log4j.properties
add jar HiveKudu-Handler-0.0.1.jar
Added [HiveKudu-Handler-0.0.1.jar] to class path
Added resources: [HiveKudu-Handler-0.0.1.jar]
add jar kudu-client-0.6.0.jar
Added [kudu-client-0.6.0.jar] to class path
Added resources: [kudu-client-0.6.0.jar]
add jar async-1.3.1.jar
Added [async-1.3.1.jar] to class path
Added resources: [async-1.3.1.jar]
set hive.cli.print.header=true


CREATE TABLE if not exists test_drop (
id INT,
name STRING
)
stored by 'org.apache.hadoop.hive.kududb.KuduHandler.KuduStorageHandler'
TBLPROPERTIES(
  'kudu.table_name' = 'test_drop',
  'kudu.master_addresses' = 'ip-172-31-56-74.ec2.internal:7051',
  'kudu.key_columns' = 'id'
)
OK
Time taken: 2.924 seconds


describe formatted test_drop
OK
col_name	data_type	comment
# col_name            	data_type           	comment             
	 	 
id                  	int                 	from deserializer   
name                	string              	from deserializer   
	 	 
# Detailed Table Information	 	 
Database:           	default             	 
Owner:              	hdfs                	 
CreateTime:         	Fri Apr 15 00:45:42 EDT 2016	 
LastAccessTime:     	UNKNOWN             	 
Protect Mode:       	None                	 
Retention:          	0                   	 
Location:           	hdfs://ip-172-31-56-74.ec2.internal:8020/user/hive/warehouse/test_drop	 
Table Type:         	MANAGED_TABLE       	 
Table Parameters:	 	 
	kudu.key_columns    	id                  
	kudu.master_addresses	ip-172-31-56-74.ec2.internal:7051
	kudu.table_name     	test_drop           
	storage_handler     	org.apache.hadoop.hive.kududb.KuduHandler.KuduStorageHandler
	transient_lastDdlTime	1460695542          
	 	 
# Storage Information	 	 
SerDe Library:      	org.apache.hadoop.hive.kududb.KuduHandler.HiveKuduSerDe	 
InputFormat:        	null                	 
OutputFormat:       	null                	 
Compressed:         	No                  	 
Num Buckets:        	-1                  	 
Bucket Columns:     	[]                  	 
Sort Columns:       	[]                  	 
Storage Desc Params:	 	 
	serialization.format	1                   
Time taken: 0.277 seconds, Fetched: 31 row(s)


insert into test_drop values (1, 'a'), (2, 'b'), (3, 'a')
Query ID = hdfs_20160415004545_5d94fdd4-d6e1-4fe3-b6ef-29eda4f309e5
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1460484956690_0052, Tracking URL = http://ip-172-31-56-74.ec2.internal:8088/proxy/application_1460484956690_0052/
Kill Command = /opt/cloudera/parcels/CDH-5.5.2-1.cdh5.5.2.p0.4/lib/hadoop/bin/hadoop job  -kill job_1460484956690_0052
Hadoop job information for Stage-0: number of mappers: 1; number of reducers: 0
2016-04-15 00:45:53,003 Stage-0 map = 0%,  reduce = 0%
2016-04-15 00:46:00,375 Stage-0 map = 100%,  reduce = 0%, Cumulative CPU 1.73 sec
MapReduce Total cumulative CPU time: 1 seconds 730 msec
Ended Job = job_1460484956690_0052
MapReduce Jobs Launched: 
Stage-Stage-0: Map: 1   Cumulative CPU: 1.73 sec   HDFS Read: 3934 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 1 seconds 730 msec
OK
_col0	_col1
Time taken: 18.704 seconds


select count(*) from test_drop
Query ID = hdfs_20160415004646_ee73a7b3-1beb-4dc7-b102-aa5ccf322f10
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1460484956690_0053, Tracking URL = http://ip-172-31-56-74.ec2.internal:8088/proxy/application_1460484956690_0053/
Kill Command = /opt/cloudera/parcels/CDH-5.5.2-1.cdh5.5.2.p0.4/lib/hadoop/bin/hadoop job  -kill job_1460484956690_0053
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2016-04-15 00:46:09,350 Stage-1 map = 0%,  reduce = 0%
2016-04-15 00:46:15,773 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.39 sec
2016-04-15 00:46:24,094 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 2.98 sec
MapReduce Total cumulative CPU time: 2 seconds 980 msec
Ended Job = job_1460484956690_0053
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 2.98 sec   HDFS Read: 6865 HDFS Write: 2 SUCCESS
Total MapReduce CPU Time Spent: 2 seconds 980 msec
OK
_c0
3
Time taken: 23.661 seconds, Fetched: 1 row(s)


select id from test_Drop where name = 'a'
Query ID = hdfs_20160415004646_fc52eb30-7464-4ff3-a83f-91b0db8d73df
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1460484956690_0054, Tracking URL = http://ip-172-31-56-74.ec2.internal:8088/proxy/application_1460484956690_0054/
Kill Command = /opt/cloudera/parcels/CDH-5.5.2-1.cdh5.5.2.p0.4/lib/hadoop/bin/hadoop job  -kill job_1460484956690_0054
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2016-04-15 00:46:32,780 Stage-1 map = 0%,  reduce = 0%
2016-04-15 00:46:40,051 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.11 sec
MapReduce Total cumulative CPU time: 2 seconds 110 msec
Ended Job = job_1460484956690_0054
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 2.11 sec   HDFS Read: 3991 HDFS Write: 4 SUCCESS
Total MapReduce CPU Time Spent: 2 seconds 110 msec
OK
id
1
3
Time taken: 15.94 seconds, Fetched: 2 row(s)


select name, count(*) from test_drop group by name
Query ID = hdfs_20160415004646_15757794-72c2-45a7-84b4-ba7b0a5d4405
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1460484956690_0055, Tracking URL = http://ip-172-31-56-74.ec2.internal:8088/proxy/application_1460484956690_0055/
Kill Command = /opt/cloudera/parcels/CDH-5.5.2-1.cdh5.5.2.p0.4/lib/hadoop/bin/hadoop job  -kill job_1460484956690_0055
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2016-04-15 00:46:48,532 Stage-1 map = 0%,  reduce = 0%
2016-04-15 00:46:55,742 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.4 sec
2016-04-15 00:47:02,987 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 2.84 sec
MapReduce Total cumulative CPU time: 2 seconds 840 msec
Ended Job = job_1460484956690_0055
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 2.84 sec   HDFS Read: 7341 HDFS Write: 8 SUCCESS
Total MapReduce CPU Time Spent: 2 seconds 840 msec
OK
name	_c1
a	2
b	1
Time taken: 22.899 seconds, Fetched: 2 row(s)


drop table test_Drop
OK
Time taken: 0.113 seconds
WARN: The method class org.apache.commons.logging.impl.SLF4JLogFactory#release() was invoked.
WARN: Please see http://www.slf4j.org/codes.html#release for an explanation.

```
