use mounica;

drop table Departments;
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

drop table titles;
CREATE EXTERNAL TABLE titles
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114229/hive/exl/titles"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114229/new/titles.avsc');


select * from titles;



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
 
select * from employeesorg;



--1) A list showing employee number, last name, first name, sex, and salary for each employee1.

select s.emp_no, e.last_name, e.first_name, e.sex, s.salary
from employeesorg as e
inner join Salaries as s
on s.emp_no = e.emp_no
order by s.emp_no;

---1) A list showing first name, last name, and hire date for employees who were hired in 1986

select emp_no, last_name, first_name, hire_date 
from employeesorg 
where cast(substr( hire_date,7,4) as int) = '1986';

--2) A list showing the manager of each department with the following information: department number, department name,the manager's employee number, last name, first name.


select distinct  department_Managers.dept_no, Departments.dept_name, 
Department_Managers.emp_no, employeesorg.last_name, employeesorg.first_name
from Department_Managers 
inner join Departments 
on Department_Managers.dept_no= Departments.dept_no
inner join employeesorg 
on Department_Managers.emp_no = employeesorg.emp_no
order by Department_Managers.dept_no;


--3) A list showing the department of each employee with the following information: employee number, last name, first name, and department name

select distinct  E.emp_no, E.last_name, E.first_name, D.dept_no, Departments.dept_name
from employeesorg as E
left join department_employees as D
on E.emp_no = D.emp_no
inner join Departments
on D.dept_no = Departments.dept_no
order by E.emp_no;

--4) A list showing first name, last name, and sex for employees whose first name is "Hercules" and last names begin with "B.“
select last_name, first_name
from employeesorg 
where (first_name = 'Hercules') and (lower(last_name) like 'b%')
order by last_name;

--5) A list showing all employees in the Sales department, including their employee number, last name, first name, and department name.
select eo.emp_no, eo.last_name, eo.first_name, Departments.dept_name
from employeesorg as eo 
inner join department_employees as D 
on D.emp_no = eo.emp_no
inner join departments on D.dept_no= departments.dept_no
where lower(departments.dept_name) = 'sales';


--6) A list showing all employees in the Sales and Development departments, including their employee number, last name, first name, and department name.
select eo.emp_no, eo.last_name, eo.first_name, departments.dept_name
from employeesorg as eo 
inner join department_employees 
on department_employees.emp_no = eo.emp_no
inner join departments on department_employees.dept_no= departments.dept_no
where (lower(departments.dept_name) = 'sales') or (lower(departments.dept_name) = 'development');

--7) A list showing the frequency count of employee last names, in descending order. ( i.e., how many employees share each last name).
select last_name,count(last_name) as Frequency 
from employeesorg 
group by last_name
order by Frequency desc;

--8. Histogram to show the salary distribution among the employees

drop view BINS;
CREATE view BINS as
SELECT
    CASE 
        WHEN s.salary >= 40000 and s.salary < 50000 THEN '40k-50k'
        WHEN s.salary >= 50000 and s.salary <60000 THEN '50k-60k'
        WHEN s.salary >= 60000 and s.salary < 70000 THEN '60k-70k'
        WHEN s.salary >= 70000 and s.salary < 80000 THEN '70k-80k'
        WHEN s.salary >= 80000 and s.salary < 90000 THEN '80k-90k'
        WHEN s.salary >= 90000 and s.salary < 100000 THEN '90k-100k'
        WHEN s.salary >= 100000 and s.salary < 110000 THEN '100k-110k'
        WHEN s.salary >= 110000 and s.salary < 120000 THEN '110k-120k'
        WHEN s.salary >= 120000 and s.salary < 130000 THEN '120k-130k'
        ELSE 'NA'
        END as Bins
FROM employeesorg e
JOIN salaries s
on s.emp_no = e.emp_no;

SELECT bins,count(bins) as salary_range
from bins
GROUP BY bins
ORDER BY salary_range DESC;


--9. Bar graph to show the Average salary per title (designation)

SELECT
    t.title,
    avg(s.salary) as Avg_Salary
FROM employees e
JOIN titles t
ON t.title_id = e.emp_title_id
JOIN salaries s
ON s.emp_no = e.emp_no
GROUP BY t.title;

--10 Calculate employee tenure & show the tenure distribution among the employees

CREATE table Employees_final as
SELECT
    emp_no as emp_no,
    emp_title_id as emp_title_id,
    cast(concat(substring_index((substring_index(birth_date,'/',3)),'/',-1),'-',
            substring_index((substring_index(birth_date,'/',1)),'/',-1),'-',
            substring_index((substring_index(birth_date,'/',2)),'/',-1)) as DATE) as birth_date,
    first_name as first_name,
    last_name as last_name,
    sex as sex,
    cast(concat(substring_index((substring_index(hire_date,'/',3)),'/',-1),'-',
            substring_index((substring_index(hire_date,'/',1)),'/',-1),'-',
            substring_index((substring_index(hire_date,'/',2)),'/',-1)) as DATE) as Hire_date,
    no_of_projects as no_of_projects,
    last_performance_ratings as last_performance_ratings,
    cast(left_org as int) as left_org,
     cast(concat(substring_index((substring_index(last_date,'/',3)),'/',-1),'-',
            substring_index((substring_index(last_date,'/',1)),'/',-1),'-',
            substring_index((substring_index(last_date,'/',2)),'/',-1)) as DATE)
from employeesorg;
SELECT  CONCAT(first_name," ",last_name," ") 
AS EmployeeName,emp_no,2000-year(hire_date) as tenure
FROM employees_final 
order by tenure desc;

--11 A list to show the emp no and dept name

SELECT e.emp_no,d.dept_name
from department_employees as e
JOIN departments as d
on e.dept_no = d.dept_no;


--12. A list showing employee firstname,last name and salary

select e.first_name,e.last_name,s.salary
from employeesorg as e
join salaries as s
on e.emp_no = s.emp_no;

--13 A list showing the number of employees working in each department

select count(d.dept_no) as Number_of_employees, de.dept_name
from departments as de
join department_employees as d 
on de.dept_no = d.dept_no
group by de.dept_name;

--14 A list concating first name and last name of employees


SELECT CONCAT(first_name," ",last_name," ") 
AS EmployeeName from employeesorg;

--15 A list to show employee name and their designation

SELECT CONCAT(e.first_name," ",e.last_name," ") 
AS EmployeeName, t.title as Destination from employeesorg as e
join titles as t
on e.emp_title_id = t.title_id;


--16  what is the highest salary for the employees

select max(salary) as highest_salary from salaries
