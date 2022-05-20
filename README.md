# CAPSTONE PROJECT DOCUMENTATION

# BUSINESS OBJECTIVE

As a process of business objective to this project it is said to remain of the database of employees from that period are six CSV files. In this project, you will design the tables to hold data in the CSVs, import the CSVs into a SQL database, import to HDFS/Hive, and perform analysis using Hive/Impala/Spark/SparkML using the data and create pipelines.

# Technology Stack
As a part of technology stack you are required to work on below technology Stack. 
- MySQL (to create database) 
- Linux Commands 
- Sqoop (Transfer data from MySQL Server to HDFS/Hive) 
- HDFS (to store the data) 
- Hive (to create database) 
- Impala (to perform the EDA) 
- SparkSQL (to perform the EDA) 
- SparkML (to perform model building


# DATA USED

-Departments
- dept_emp
-dept_manager
-employees
-salaries
-titles

# DATA DESCRIPTION

# Titles (titles.csv)

This data contains title id and title

# Employees (employees.csv)

This data contains Employee Id,designation id, Date of Birth,First Name,Last Name,Gender,Employee Hire date,Number of projects worked on, Last year performance rating ,Employee left the organization,Last date of employment (Exit Date) 

# Salaries (salaries.csv)

This data contains Employee id,Employee’s Salary

# Departments (departments.csv)

This data contains Unique id for each department,Department Name

# Department Managers (dept_manager.csv) 

This data contains Unique id for each department ,Employee number (head of the department )

# Department Employees (dept_emp.csv)

This data contains Employee id,Unique id for each department

# PROJECT OBJECTIVE: 

 As part of this project, you are required to work on 
1. Create data model as per your understanding from the data (you are required include tables names, relation between tables, column names, data types, primary & foreign keys etc.) Tip: You can create ER diagram either in EXCEL or using the link https://www.quickdatabasediagrams.com/ (Preferably in this app) 
2. Create database & tables in MySQL server as per the above ER Diagram 
3. Create Sqoop job to transfer the data from MySQL to HDFS (Data required to store in Parque/Avro/Json format) 
4. Create database in Hive as per the above ER Diagram and load the data into Hive tables 
5. Work on Exploratory data analysis as per the analysis requirement using Impala and Spark SQL. 
6. Build ML Model as per the requirement.
7. Create entire data pipeline and ML pipe line 
8. Upload the entire project repository including source code to Github (you are required to create github account if you don’t have it

# CREATING THE TABLES

a)create table titles(              
title_id varchar(30) primary key,
title varchar(20));

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
 

# LOADING THE DATA

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

# IMPORT THE SQOOP FILES USING SHELL


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table titles --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table Department_Employees --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table Department_Managers --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table Departments --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver

sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table employees --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver


sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --table employees --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver --m 1 --driver com.mysql.jdbc.Driver

sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114229 --username anabig114229 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114229/hive/exl --driver com.mysql.jdbc.Driver 

# The other processes are given in the form of jupyter notebook and sql formats
