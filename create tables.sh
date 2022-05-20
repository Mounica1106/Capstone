create the tables

a)create table titles(                                                                   title_id varchar(30) primary key,                                                      title varchar(20));

b)create table Employees(
emp_no Int,
emp_titles_id varchar(30),
birth_date varchar(20),
first_name varchar(70),
last_name varchar(70),
sex varchar(20),
hire_date varchar(20),
no_of_projects Int,
Last_performance_rating varchar(40),
left_organisation boolean,
Last_date date);

c)create table Salaries (
emp_no Int,
Salary Int);

d)create table Departments (
dept_no varchar(30),
dept_name varchar(40));

e)create table Department_Managers (
dept_no varchar(40),
emp_no Int);

f)create table Department_Employees (
emp_no Int,
dept_no varchar(30));
 

4) load the data

a) load data local infile '/home/anabig114229/titles.csv' into table titles fields terminated by ',' enclosed by '"' lines terminate
d by '\n' ignore 1 rows;

b)load data local infile '/home/anabig114229/employees.csv' into table employees fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
 
c) load data local infile '/home/anabig114229/salaries.csv' into table Salaries fields terminated by ',' enclosed by '"' lines terminate
d by '\n' ignore 1 rows;

d)load data local infile '/home/anabig114229/departments.csv' into table Departments fields terminated by ',' enclosed by '"' lines terminate
d by '\n' ignore 1 rows;
 
e)load data local infile '/home/anabig114229/dept_managers.csv' into table Department_Managers fields terminated by ',' enclosed by '"' lines terminate
d by '\n' ignore 1 rows;
 
f) load data local infile '/home/anabig114229/dept_emp.csv' into table Department_Employees fields terminated by ',' enclosed by '"' lines terminate
d by '\n' ignore 1 rows;

5) import the sqoop files using shell


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table titles --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table Department_Employees --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table Department_Managers --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table Departments --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver

sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table employees --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table employees --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver

sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver 

6) Check the data is moved to HDFS or not

hdfs dfs -ls /user/anabig114229/hive/exl 

7)Check the avsc schema files created or not

ls -l /home/anabig114229/*.avsc

8) create a new directory and transfer the avsc files

hadoop fs -mkdir /user/anabig114229/new

hadoop fs -put /home/anabig114229/titles.avsc /user/anabig114229/new/titles.avsc
hadoop fs -put /home/anabig114229/Salaries.avsc /user/anabig114229/new/Salaries.avsc
hadoop fs -put /home/anabig114229/Departments.avsc /user/anabig114229/new/Departments.avsc
hadoop fs -put /home/anabig114229/Department_Employees.avsc /user/anabig114229/new/Department_Employees.avsc
hadoop fs -put /home/anabig114229/Department_Managers.avsc /user/anabig114229/new/Department_Managers.avsc
hadoop fs -put /home/anabig114229/Employees.avsc /user/anabig114229/new/Employees.avsc
hadoop fs -mkdir /user/anabig114229/impala

9) check for the transfer

hdfs dfs -ls /user/anabig114229/new

hdfs dfs -ls /user/anabig114229/new
hadoop fs -chmod +rw /user/anabig114229/new





10) create the tables in hive

CREATE EXTERNAL TABLE Departments
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114229/hive/exl/Departments"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114229/new/Departments.avsc');


select * from Departments;


CREATE EXTERNAL TABLE Salaries
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114229/hive/exl/Salaries"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114229/new/Salaries.avsc');


select * from Salaries;


CREATE EXTERNAL TABLE Department_Managers
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114229/hive/exl/Department_Managers"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114229/new/Department_Managers.avsc');


select * from Department_Managers;

CREATE EXTERNAL TABLE Department_Employees
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114229/hive/exl/Department_Employees"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114229/new/Department_Employees.avsc');



select * from Department_Employees;


CREATE EXTERNAL TABLE employeesorg
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114229/hive/exl/employees"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114229/new/employees.avsc');


CREATE EXTERNAL TABLE titles
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114229/hive/exl/titles"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114229/new/titles.avsc');



select * from titles;

