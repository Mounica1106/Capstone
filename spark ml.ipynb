{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "Spark = (SparkSession.builder.appName(\"capstone_ML\")\\\n",
    "        .config(\"hive.metastore.uris\",\"thrift://ip-10-1-2-24.ap-south-1.compute.internal:9083\")\\\n",
    "        .enableHiveSupport().getOrCreate())"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Read all the tables"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "titles = Spark.sql(\"select * from mounica.titles\")\n",
    "departments =  Spark.sql(\"select * from mounica.Departments\")\n",
    "employees = Spark.sql(\"select * from mounica.employeesorg\")\n",
    "department_employees = Spark.sql(\"select * from mounica.Department_Employees\")\n",
    "department_managers = Spark.sql(\"select * from mounica.Department_Managers\")\n",
    "salaries = Spark.sql(\"select * from mounica.Salaries\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------+------------------+\n",
      "|title_id|             title|\n",
      "+--------+------------------+\n",
      "|   e0001|Assistant Engineer|\n",
      "|   e0002|          Engineer|\n",
      "|   e0003|   Senior Engineer|\n",
      "|   e0004|  Technique Leader|\n",
      "|   m0001|Managerï»¿title_id|\n",
      "|   s0001|             Staff|\n",
      "|   s0002|      Senior Staff|\n",
      "+--------+------------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "titles.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------+------------------+\n",
      "|dept_no|         dept_name|\n",
      "+-------+------------------+\n",
      "|   d001|         Marketing|\n",
      "|   d002|           Finance|\n",
      "|   d003|     HumanResource|\n",
      "|   d004|        Production|\n",
      "|   d005|       development|\n",
      "|   d006|Quality Management|\n",
      "|   d007|             Sales|\n",
      "|   d008|          Research|\n",
      "|   d009|  Customer Service|\n",
      "+-------+------------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "departments.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+------------+----------+----------+----------+---+----------+--------------+------------------------+--------+----------+\n",
      "|emp_no|emp_title_id|birth_date|first_name| last_name|sex| hire_date|no_of_projects|last_performance_ratings|left_org| last_date|\n",
      "+------+------------+----------+----------+----------+---+----------+--------------+------------------------+--------+----------+\n",
      "|473302|       s0001| 7/25/1953|  Hideyuki|  Zallocco|  M| 4/28/1990|             2|                       A|       0|         \r",
      "|\n",
      "|475053|       e0002|11/18/1954|     Byong| Delgrande|  F|  9/7/1991|             1|                       C|       0|         \r",
      "|\n",
      "| 57444|       e0002| 1/30/1958|     Berry|      Babb|  F| 3/21/1992|             9|                       A|       0|         \r",
      "|\n",
      "|421786|       s0001| 9/28/1957|     Xiong|  Verhoeff|  M|11/26/1987|             2|                       C|       0|         \r",
      "|\n",
      "|282238|       e0003|10/28/1952|Abdelkader|   Baumann|  F| 1/18/1991|             6|                       B|       0|         \r",
      "|\n",
      "|263976|       e0003|10/30/1959|      Eran|  Cusworth|  M|11/14/1986|             8|                       B|       1|8/21/1993\r",
      "|\n",
      "|273487|       s0001| 4/14/1957| Christoph|   Parfitt|  M| 6/28/1991|            10|                       A|       0|         \r",
      "|\n",
      "|461591|       s0002|11/17/1964|    Xudong|  Samarati|  M|11/13/1985|             2|                       A|       1|8/21/1994\r",
      "|\n",
      "|477657|       e0002|12/18/1962|    Lihong| Magliocco|  M|10/23/1993|             5|                       A|       0|         \r",
      "|\n",
      "|219881|       s0002| 4/24/1956| Kwangyoen|     Speek|  F| 2/14/1993|             6|                       B|       0|         \r",
      "|\n",
      "| 29920|       e0002|12/31/1961|   Shuichi|     Tyugu|  F| 1/17/1995|             1|                     PIP|       0|         \r",
      "|\n",
      "|208153|       e0003|10/25/1961|   Abdulah|      Lunn|  M|  4/8/1989|             8|                       C|       1| 8/2/2000\r",
      "|\n",
      "| 13616|       e0003| 8/30/1961|     Perry|     Lorho|  F|  8/3/1991|             7|                       C|       0|         \r",
      "|\n",
      "|246449|       s0001| 3/23/1958|     Subbu|Bultermann|  F| 3/25/1988|             9|                       C|       0|         \r",
      "|\n",
      "| 21529|       e0002| 5/19/1959|     Bojan|  Zallocco|  M|10/14/1986|             7|                       C|       0|         \r",
      "|\n",
      "| 17934|       e0004| 7/12/1963|  Bilhanan|  Wuwongse|  M| 10/6/1993|             7|                       C|       0|         \r",
      "|\n",
      "| 48085|       s0001| 1/19/1964|Venkatesan|      Gilg|  M| 6/28/1993|             9|                       B|       0|         \r",
      "|\n",
      "|239838|       e0002|12/11/1957|   Naftali|     Dulli|  M|  6/6/1993|             3|                       A|       0|         \r",
      "|\n",
      "|240129|       e0004| 8/11/1952|     Roddy|    Karnin|  M| 5/29/1985|            10|                       C|       0|         \r",
      "|\n",
      "|205246|       s0002|11/25/1958|     Nevio|    Demizu|  F| 5/18/1986|             8|                       B|       0|         \r",
      "|\n",
      "+------+------------+----------+----------+----------+---+----------+--------------+------------------------+--------+----------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "employees.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------+------+\n",
      "|dept_no|emp_no|\n",
      "+-------+------+\n",
      "|   d001|110022|\n",
      "|   d001|110039|\n",
      "|   d002|110085|\n",
      "|   d002|110114|\n",
      "|   d003|110183|\n",
      "|   d003|110228|\n",
      "|   d004|110303|\n",
      "|   d004|110344|\n",
      "|   d004|110386|\n",
      "|   d004|110420|\n",
      "|   d005|110511|\n",
      "|   d005|110567|\n",
      "|   d006|110725|\n",
      "|   d006|110765|\n",
      "|   d006|110800|\n",
      "|   d006|110854|\n",
      "|   d007|111035|\n",
      "|   d007|111133|\n",
      "|   d008|111400|\n",
      "|   d008|111534|\n",
      "+-------+------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "department_managers.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+-------+\n",
      "|emp_no|dept_no|\n",
      "+------+-------+\n",
      "| 10001|   d005|\n",
      "| 10002|   d007|\n",
      "| 10003|   d004|\n",
      "| 10004|   d004|\n",
      "| 10005|   d003|\n",
      "| 10006|   d005|\n",
      "| 10007|   d008|\n",
      "| 10008|   d005|\n",
      "| 10009|   d006|\n",
      "| 10010|   d004|\n",
      "| 10010|   d006|\n",
      "| 10011|   d009|\n",
      "| 10012|   d005|\n",
      "| 10013|   d003|\n",
      "| 10014|   d005|\n",
      "| 10015|   d008|\n",
      "| 10016|   d007|\n",
      "| 10017|   d001|\n",
      "| 10018|   d004|\n",
      "| 10018|   d005|\n",
      "+------+-------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "department_employees.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+------+\n",
      "|emp_no|salary|\n",
      "+------+------+\n",
      "| 10001| 60117|\n",
      "| 10002| 65828|\n",
      "| 10003| 40006|\n",
      "| 10004| 40054|\n",
      "| 10005| 78228|\n",
      "| 10006| 40000|\n",
      "| 10007| 56724|\n",
      "| 10008| 46671|\n",
      "| 10009| 60929|\n",
      "| 10010| 72488|\n",
      "| 10011| 42365|\n",
      "| 10012| 40000|\n",
      "| 10013| 40000|\n",
      "| 10014| 46168|\n",
      "| 10015| 40000|\n",
      "| 10016| 70889|\n",
      "| 10017| 71380|\n",
      "| 10018| 55881|\n",
      "| 10019| 44276|\n",
      "| 10020| 40000|\n",
      "+------+------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "salaries.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Join all the tables at employees level"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "metadata": {},
   "outputs": [],
   "source": [
    "df = employees.join(department_employees, on='emp_no', how='left')\\\n",
    "               .join(departments,departments.dept_no == department_employees.dept_no, how='left')\\\n",
    "               .join(department_managers, on='emp_no', how='left')\\\n",
    "               .join(salaries, on='emp_no', how='left')\\\n",
    "               .join(titles,employees.emp_title_id==titles.title_id, how='left')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 41,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>emp_no</th>\n",
       "      <th>emp_title_id</th>\n",
       "      <th>birth_date</th>\n",
       "      <th>first_name</th>\n",
       "      <th>last_name</th>\n",
       "      <th>sex</th>\n",
       "      <th>hire_date</th>\n",
       "      <th>no_of_projects</th>\n",
       "      <th>last_performance_ratings</th>\n",
       "      <th>left_org</th>\n",
       "      <th>last_date</th>\n",
       "      <th>dept_no</th>\n",
       "      <th>dept_no</th>\n",
       "      <th>dept_name</th>\n",
       "      <th>dept_no</th>\n",
       "      <th>salary</th>\n",
       "      <th>title_id</th>\n",
       "      <th>title</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>473302</td>\n",
       "      <td>s0001</td>\n",
       "      <td>7/25/1953</td>\n",
       "      <td>Hideyuki</td>\n",
       "      <td>Zallocco</td>\n",
       "      <td>M</td>\n",
       "      <td>4/28/1990</td>\n",
       "      <td>2</td>\n",
       "      <td>A</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d002</td>\n",
       "      <td>d002</td>\n",
       "      <td>Finance</td>\n",
       "      <td>None</td>\n",
       "      <td>40000</td>\n",
       "      <td>s0001</td>\n",
       "      <td>Staff</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>475053</td>\n",
       "      <td>e0002</td>\n",
       "      <td>11/18/1954</td>\n",
       "      <td>Byong</td>\n",
       "      <td>Delgrande</td>\n",
       "      <td>F</td>\n",
       "      <td>9/7/1991</td>\n",
       "      <td>1</td>\n",
       "      <td>C</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d004</td>\n",
       "      <td>d004</td>\n",
       "      <td>Production</td>\n",
       "      <td>None</td>\n",
       "      <td>53422</td>\n",
       "      <td>e0002</td>\n",
       "      <td>Engineer</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>57444</td>\n",
       "      <td>e0002</td>\n",
       "      <td>1/30/1958</td>\n",
       "      <td>Berry</td>\n",
       "      <td>Babb</td>\n",
       "      <td>F</td>\n",
       "      <td>3/21/1992</td>\n",
       "      <td>9</td>\n",
       "      <td>A</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d004</td>\n",
       "      <td>d004</td>\n",
       "      <td>Production</td>\n",
       "      <td>None</td>\n",
       "      <td>48973</td>\n",
       "      <td>e0002</td>\n",
       "      <td>Engineer</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>421786</td>\n",
       "      <td>s0001</td>\n",
       "      <td>9/28/1957</td>\n",
       "      <td>Xiong</td>\n",
       "      <td>Verhoeff</td>\n",
       "      <td>M</td>\n",
       "      <td>11/26/1987</td>\n",
       "      <td>2</td>\n",
       "      <td>C</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d003</td>\n",
       "      <td>d003</td>\n",
       "      <td>HumanResource</td>\n",
       "      <td>None</td>\n",
       "      <td>40000</td>\n",
       "      <td>s0001</td>\n",
       "      <td>Staff</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>282238</td>\n",
       "      <td>e0003</td>\n",
       "      <td>10/28/1952</td>\n",
       "      <td>Abdelkader</td>\n",
       "      <td>Baumann</td>\n",
       "      <td>F</td>\n",
       "      <td>1/18/1991</td>\n",
       "      <td>6</td>\n",
       "      <td>B</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d006</td>\n",
       "      <td>d006</td>\n",
       "      <td>Quality Management</td>\n",
       "      <td>None</td>\n",
       "      <td>40000</td>\n",
       "      <td>e0003</td>\n",
       "      <td>Senior Engineer</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>...</th>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>331598</th>\n",
       "      <td>255832</td>\n",
       "      <td>e0002</td>\n",
       "      <td>5/8/1955</td>\n",
       "      <td>Yuping</td>\n",
       "      <td>Dayang</td>\n",
       "      <td>F</td>\n",
       "      <td>2/26/1995</td>\n",
       "      <td>10</td>\n",
       "      <td>A</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d004</td>\n",
       "      <td>d004</td>\n",
       "      <td>Production</td>\n",
       "      <td>None</td>\n",
       "      <td>75355</td>\n",
       "      <td>e0002</td>\n",
       "      <td>Engineer</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>331599</th>\n",
       "      <td>76671</td>\n",
       "      <td>s0001</td>\n",
       "      <td>6/9/1959</td>\n",
       "      <td>Ortrud</td>\n",
       "      <td>Plessier</td>\n",
       "      <td>M</td>\n",
       "      <td>2/24/1988</td>\n",
       "      <td>8</td>\n",
       "      <td>B</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d007</td>\n",
       "      <td>d007</td>\n",
       "      <td>Sales</td>\n",
       "      <td>None</td>\n",
       "      <td>61886</td>\n",
       "      <td>s0001</td>\n",
       "      <td>Staff</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>331600</th>\n",
       "      <td>264920</td>\n",
       "      <td>s0001</td>\n",
       "      <td>9/22/1959</td>\n",
       "      <td>Percy</td>\n",
       "      <td>Samarati</td>\n",
       "      <td>F</td>\n",
       "      <td>9/8/1994</td>\n",
       "      <td>1</td>\n",
       "      <td>B</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d007</td>\n",
       "      <td>d007</td>\n",
       "      <td>Sales</td>\n",
       "      <td>None</td>\n",
       "      <td>62772</td>\n",
       "      <td>s0001</td>\n",
       "      <td>Staff</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>331601</th>\n",
       "      <td>264920</td>\n",
       "      <td>s0001</td>\n",
       "      <td>9/22/1959</td>\n",
       "      <td>Percy</td>\n",
       "      <td>Samarati</td>\n",
       "      <td>F</td>\n",
       "      <td>9/8/1994</td>\n",
       "      <td>1</td>\n",
       "      <td>B</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d002</td>\n",
       "      <td>d002</td>\n",
       "      <td>Finance</td>\n",
       "      <td>None</td>\n",
       "      <td>62772</td>\n",
       "      <td>s0001</td>\n",
       "      <td>Staff</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>331602</th>\n",
       "      <td>464503</td>\n",
       "      <td>s0002</td>\n",
       "      <td>5/31/1964</td>\n",
       "      <td>Arvind</td>\n",
       "      <td>Slobodova</td>\n",
       "      <td>M</td>\n",
       "      <td>11/23/1987</td>\n",
       "      <td>8</td>\n",
       "      <td>C</td>\n",
       "      <td>0</td>\n",
       "      <td>\\r</td>\n",
       "      <td>d008</td>\n",
       "      <td>d008</td>\n",
       "      <td>Research</td>\n",
       "      <td>None</td>\n",
       "      <td>41708</td>\n",
       "      <td>s0002</td>\n",
       "      <td>Senior Staff</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "<p>331603 rows × 18 columns</p>\n",
       "</div>"
      ],
      "text/plain": [
       "        emp_no emp_title_id  birth_date  first_name  last_name sex  \\\n",
       "0       473302        s0001   7/25/1953    Hideyuki   Zallocco   M   \n",
       "1       475053        e0002  11/18/1954       Byong  Delgrande   F   \n",
       "2        57444        e0002   1/30/1958       Berry       Babb   F   \n",
       "3       421786        s0001   9/28/1957       Xiong   Verhoeff   M   \n",
       "4       282238        e0003  10/28/1952  Abdelkader    Baumann   F   \n",
       "...        ...          ...         ...         ...        ...  ..   \n",
       "331598  255832        e0002    5/8/1955      Yuping     Dayang   F   \n",
       "331599   76671        s0001    6/9/1959      Ortrud   Plessier   M   \n",
       "331600  264920        s0001   9/22/1959       Percy   Samarati   F   \n",
       "331601  264920        s0001   9/22/1959       Percy   Samarati   F   \n",
       "331602  464503        s0002   5/31/1964      Arvind  Slobodova   M   \n",
       "\n",
       "         hire_date  no_of_projects last_performance_ratings left_org  \\\n",
       "0        4/28/1990               2                        A        0   \n",
       "1         9/7/1991               1                        C        0   \n",
       "2        3/21/1992               9                        A        0   \n",
       "3       11/26/1987               2                        C        0   \n",
       "4        1/18/1991               6                        B        0   \n",
       "...            ...             ...                      ...      ...   \n",
       "331598   2/26/1995              10                        A        0   \n",
       "331599   2/24/1988               8                        B        0   \n",
       "331600    9/8/1994               1                        B        0   \n",
       "331601    9/8/1994               1                        B        0   \n",
       "331602  11/23/1987               8                        C        0   \n",
       "\n",
       "       last_date dept_no dept_no           dept_name dept_no  salary title_id  \\\n",
       "0             \\r    d002    d002             Finance    None   40000    s0001   \n",
       "1             \\r    d004    d004          Production    None   53422    e0002   \n",
       "2             \\r    d004    d004          Production    None   48973    e0002   \n",
       "3             \\r    d003    d003       HumanResource    None   40000    s0001   \n",
       "4             \\r    d006    d006  Quality Management    None   40000    e0003   \n",
       "...          ...     ...     ...                 ...     ...     ...      ...   \n",
       "331598        \\r    d004    d004          Production    None   75355    e0002   \n",
       "331599        \\r    d007    d007               Sales    None   61886    s0001   \n",
       "331600        \\r    d007    d007               Sales    None   62772    s0001   \n",
       "331601        \\r    d002    d002             Finance    None   62772    s0001   \n",
       "331602        \\r    d008    d008            Research    None   41708    s0002   \n",
       "\n",
       "                  title  \n",
       "0                 Staff  \n",
       "1              Engineer  \n",
       "2              Engineer  \n",
       "3                 Staff  \n",
       "4       Senior Engineer  \n",
       "...                 ...  \n",
       "331598         Engineer  \n",
       "331599            Staff  \n",
       "331600            Staff  \n",
       "331601            Staff  \n",
       "331602     Senior Staff  \n",
       "\n",
       "[331603 rows x 18 columns]"
      ]
     },
     "execution_count": 41,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df.toPandas()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 42,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>0</th>\n",
       "      <th>1</th>\n",
       "      <th>2</th>\n",
       "      <th>3</th>\n",
       "      <th>4</th>\n",
       "      <th>5</th>\n",
       "      <th>6</th>\n",
       "      <th>7</th>\n",
       "      <th>8</th>\n",
       "      <th>9</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>emp_no</th>\n",
       "      <td>473302</td>\n",
       "      <td>475053</td>\n",
       "      <td>57444</td>\n",
       "      <td>421786</td>\n",
       "      <td>282238</td>\n",
       "      <td>263976</td>\n",
       "      <td>273487</td>\n",
       "      <td>461591</td>\n",
       "      <td>477657</td>\n",
       "      <td>219881</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>emp_title_id</th>\n",
       "      <td>s0001</td>\n",
       "      <td>e0002</td>\n",
       "      <td>e0002</td>\n",
       "      <td>s0001</td>\n",
       "      <td>e0003</td>\n",
       "      <td>e0003</td>\n",
       "      <td>s0001</td>\n",
       "      <td>s0002</td>\n",
       "      <td>e0002</td>\n",
       "      <td>s0002</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>birth_date</th>\n",
       "      <td>7/25/1953</td>\n",
       "      <td>11/18/1954</td>\n",
       "      <td>1/30/1958</td>\n",
       "      <td>9/28/1957</td>\n",
       "      <td>10/28/1952</td>\n",
       "      <td>10/30/1959</td>\n",
       "      <td>4/14/1957</td>\n",
       "      <td>11/17/1964</td>\n",
       "      <td>12/18/1962</td>\n",
       "      <td>4/24/1956</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>first_name</th>\n",
       "      <td>Hideyuki</td>\n",
       "      <td>Byong</td>\n",
       "      <td>Berry</td>\n",
       "      <td>Xiong</td>\n",
       "      <td>Abdelkader</td>\n",
       "      <td>Eran</td>\n",
       "      <td>Christoph</td>\n",
       "      <td>Xudong</td>\n",
       "      <td>Lihong</td>\n",
       "      <td>Kwangyoen</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>last_name</th>\n",
       "      <td>Zallocco</td>\n",
       "      <td>Delgrande</td>\n",
       "      <td>Babb</td>\n",
       "      <td>Verhoeff</td>\n",
       "      <td>Baumann</td>\n",
       "      <td>Cusworth</td>\n",
       "      <td>Parfitt</td>\n",
       "      <td>Samarati</td>\n",
       "      <td>Magliocco</td>\n",
       "      <td>Speek</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>sex</th>\n",
       "      <td>M</td>\n",
       "      <td>F</td>\n",
       "      <td>F</td>\n",
       "      <td>M</td>\n",
       "      <td>F</td>\n",
       "      <td>M</td>\n",
       "      <td>M</td>\n",
       "      <td>M</td>\n",
       "      <td>M</td>\n",
       "      <td>F</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>hire_date</th>\n",
       "      <td>4/28/1990</td>\n",
       "      <td>9/7/1991</td>\n",
       "      <td>3/21/1992</td>\n",
       "      <td>11/26/1987</td>\n",
       "      <td>1/18/1991</td>\n",
       "      <td>11/14/1986</td>\n",
       "      <td>6/28/1991</td>\n",
       "      <td>11/13/1985</td>\n",
       "      <td>10/23/1993</td>\n",
       "      <td>2/14/1993</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>no_of_projects</th>\n",
       "      <td>2</td>\n",
       "      <td>1</td>\n",
       "      <td>9</td>\n",
       "      <td>2</td>\n",
       "      <td>6</td>\n",
       "      <td>8</td>\n",
       "      <td>10</td>\n",
       "      <td>2</td>\n",
       "      <td>5</td>\n",
       "      <td>6</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>last_performance_ratings</th>\n",
       "      <td>A</td>\n",
       "      <td>C</td>\n",
       "      <td>A</td>\n",
       "      <td>C</td>\n",
       "      <td>B</td>\n",
       "      <td>B</td>\n",
       "      <td>A</td>\n",
       "      <td>A</td>\n",
       "      <td>A</td>\n",
       "      <td>B</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>left_org</th>\n",
       "      <td>0</td>\n",
       "      <td>0</td>\n",
       "      <td>0</td>\n",
       "      <td>0</td>\n",
       "      <td>0</td>\n",
       "      <td>1</td>\n",
       "      <td>0</td>\n",
       "      <td>1</td>\n",
       "      <td>0</td>\n",
       "      <td>0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>last_date</th>\n",
       "      <td>\\r</td>\n",
       "      <td>\\r</td>\n",
       "      <td>\\r</td>\n",
       "      <td>\\r</td>\n",
       "      <td>\\r</td>\n",
       "      <td>8/21/1993\\r</td>\n",
       "      <td>\\r</td>\n",
       "      <td>8/21/1994\\r</td>\n",
       "      <td>\\r</td>\n",
       "      <td>\\r</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>dept_no</th>\n",
       "      <td>d002</td>\n",
       "      <td>d004</td>\n",
       "      <td>d004</td>\n",
       "      <td>d003</td>\n",
       "      <td>d006</td>\n",
       "      <td>d006</td>\n",
       "      <td>d003</td>\n",
       "      <td>d002</td>\n",
       "      <td>d006</td>\n",
       "      <td>d009</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>dept_no</th>\n",
       "      <td>d002</td>\n",
       "      <td>d004</td>\n",
       "      <td>d004</td>\n",
       "      <td>d003</td>\n",
       "      <td>d006</td>\n",
       "      <td>d006</td>\n",
       "      <td>d003</td>\n",
       "      <td>d002</td>\n",
       "      <td>d006</td>\n",
       "      <td>d009</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>dept_name</th>\n",
       "      <td>Finance</td>\n",
       "      <td>Production</td>\n",
       "      <td>Production</td>\n",
       "      <td>HumanResource</td>\n",
       "      <td>Quality Management</td>\n",
       "      <td>Quality Management</td>\n",
       "      <td>HumanResource</td>\n",
       "      <td>Finance</td>\n",
       "      <td>Quality Management</td>\n",
       "      <td>Customer Service</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>dept_no</th>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>salary</th>\n",
       "      <td>40000</td>\n",
       "      <td>53422</td>\n",
       "      <td>48973</td>\n",
       "      <td>40000</td>\n",
       "      <td>40000</td>\n",
       "      <td>40000</td>\n",
       "      <td>56087</td>\n",
       "      <td>40000</td>\n",
       "      <td>54816</td>\n",
       "      <td>40000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>title_id</th>\n",
       "      <td>s0001</td>\n",
       "      <td>e0002</td>\n",
       "      <td>e0002</td>\n",
       "      <td>s0001</td>\n",
       "      <td>e0003</td>\n",
       "      <td>e0003</td>\n",
       "      <td>s0001</td>\n",
       "      <td>s0002</td>\n",
       "      <td>e0002</td>\n",
       "      <td>s0002</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>title</th>\n",
       "      <td>Staff</td>\n",
       "      <td>Engineer</td>\n",
       "      <td>Engineer</td>\n",
       "      <td>Staff</td>\n",
       "      <td>Senior Engineer</td>\n",
       "      <td>Senior Engineer</td>\n",
       "      <td>Staff</td>\n",
       "      <td>Senior Staff</td>\n",
       "      <td>Engineer</td>\n",
       "      <td>Senior Staff</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                  0           1           2              3  \\\n",
       "emp_no                       473302      475053       57444         421786   \n",
       "emp_title_id                  s0001       e0002       e0002          s0001   \n",
       "birth_date                7/25/1953  11/18/1954   1/30/1958      9/28/1957   \n",
       "first_name                 Hideyuki       Byong       Berry          Xiong   \n",
       "last_name                  Zallocco   Delgrande        Babb       Verhoeff   \n",
       "sex                               M           F           F              M   \n",
       "hire_date                 4/28/1990    9/7/1991   3/21/1992     11/26/1987   \n",
       "no_of_projects                    2           1           9              2   \n",
       "last_performance_ratings          A           C           A              C   \n",
       "left_org                          0           0           0              0   \n",
       "last_date                        \\r          \\r          \\r             \\r   \n",
       "dept_no                        d002        d004        d004           d003   \n",
       "dept_no                        d002        d004        d004           d003   \n",
       "dept_name                   Finance  Production  Production  HumanResource   \n",
       "dept_no                        None        None        None           None   \n",
       "salary                        40000       53422       48973          40000   \n",
       "title_id                      s0001       e0002       e0002          s0001   \n",
       "title                         Staff    Engineer    Engineer          Staff   \n",
       "\n",
       "                                           4                   5  \\\n",
       "emp_no                                282238              263976   \n",
       "emp_title_id                           e0003               e0003   \n",
       "birth_date                        10/28/1952          10/30/1959   \n",
       "first_name                        Abdelkader                Eran   \n",
       "last_name                            Baumann            Cusworth   \n",
       "sex                                        F                   M   \n",
       "hire_date                          1/18/1991          11/14/1986   \n",
       "no_of_projects                             6                   8   \n",
       "last_performance_ratings                   B                   B   \n",
       "left_org                                   0                   1   \n",
       "last_date                                 \\r         8/21/1993\\r   \n",
       "dept_no                                 d006                d006   \n",
       "dept_no                                 d006                d006   \n",
       "dept_name                 Quality Management  Quality Management   \n",
       "dept_no                                 None                None   \n",
       "salary                                 40000               40000   \n",
       "title_id                               e0003               e0003   \n",
       "title                        Senior Engineer     Senior Engineer   \n",
       "\n",
       "                                      6             7                   8  \\\n",
       "emp_no                           273487        461591              477657   \n",
       "emp_title_id                      s0001         s0002               e0002   \n",
       "birth_date                    4/14/1957    11/17/1964          12/18/1962   \n",
       "first_name                    Christoph        Xudong              Lihong   \n",
       "last_name                       Parfitt      Samarati           Magliocco   \n",
       "sex                                   M             M                   M   \n",
       "hire_date                     6/28/1991    11/13/1985          10/23/1993   \n",
       "no_of_projects                       10             2                   5   \n",
       "last_performance_ratings              A             A                   A   \n",
       "left_org                              0             1                   0   \n",
       "last_date                            \\r   8/21/1994\\r                  \\r   \n",
       "dept_no                            d003          d002                d006   \n",
       "dept_no                            d003          d002                d006   \n",
       "dept_name                 HumanResource       Finance  Quality Management   \n",
       "dept_no                            None          None                None   \n",
       "salary                            56087         40000               54816   \n",
       "title_id                          s0001         s0002               e0002   \n",
       "title                             Staff  Senior Staff            Engineer   \n",
       "\n",
       "                                         9  \n",
       "emp_no                              219881  \n",
       "emp_title_id                         s0002  \n",
       "birth_date                       4/24/1956  \n",
       "first_name                       Kwangyoen  \n",
       "last_name                            Speek  \n",
       "sex                                      F  \n",
       "hire_date                        2/14/1993  \n",
       "no_of_projects                           6  \n",
       "last_performance_ratings                 B  \n",
       "left_org                                 0  \n",
       "last_date                               \\r  \n",
       "dept_no                               d009  \n",
       "dept_no                               d009  \n",
       "dept_name                 Customer Service  \n",
       "dept_no                               None  \n",
       "salary                               40000  \n",
       "title_id                             s0002  \n",
       "title                         Senior Staff  "
      ]
     },
     "execution_count": 42,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df.toPandas().head(10).transpose()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 43,
   "metadata": {},
   "outputs": [],
   "source": [
    "df1 = df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Columns that will be used as features and their types\n",
    "continuous_features = ['no_of_projects','salary']\n",
    "\n",
    "categorical_features = ['last_performance_ratings', 'dept_name', 'left_org', 'title']"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 37,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Encoding all categorical features\n",
    "from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, PolynomialExpansion, VectorIndexer"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 38,
   "metadata": {},
   "outputs": [],
   "source": [
    "def create_category( dataset, field_name ):\n",
    "    idx_col = field_name + \"Index\"\n",
    "    col_vec = field_name + \"Vec\"\n",
    "\n",
    "    stringIndexer = StringIndexer( inputCol=field_name, outputCol=idx_col )\n",
    "\n",
    "    model = stringIndexer.fit( dataset )\n",
    "    indexed = model.transform( dataset )\n",
    "\n",
    "    encoder = OneHotEncoder( dropLast=True,inputCol=idx_col,outputCol= col_vec )\n",
    "\n",
    "    return encoder.transform( indexed )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 44,
   "metadata": {},
   "outputs": [],
   "source": [
    "for col in categorical_features:\n",
    "    df1 = create_category(df1, col)\n",
    "    df1.cache()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 45,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['emp_no',\n",
       " 'emp_title_id',\n",
       " 'birth_date',\n",
       " 'first_name',\n",
       " 'last_name',\n",
       " 'sex',\n",
       " 'hire_date',\n",
       " 'no_of_projects',\n",
       " 'last_performance_ratings',\n",
       " 'left_org',\n",
       " 'last_date',\n",
       " 'dept_no',\n",
       " 'dept_no',\n",
       " 'dept_name',\n",
       " 'dept_no',\n",
       " 'salary',\n",
       " 'title_id',\n",
       " 'title',\n",
       " 'last_performance_ratingsIndex',\n",
       " 'last_performance_ratingsVec',\n",
       " 'dept_nameIndex',\n",
       " 'dept_nameVec',\n",
       " 'left_orgIndex',\n",
       " 'left_orgVec',\n",
       " 'titleIndex',\n",
       " 'titleVec']"
      ]
     },
     "execution_count": 45,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df1.columns"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 46,
   "metadata": {},
   "outputs": [],
   "source": [
    "categorical_vecs = [ \"\".join( (cat, \"Vec\") ) for cat in categorical_features ]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 47,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['last_performance_ratingsVec', 'dept_nameVec', 'left_orgVec', 'titleVec']"
      ]
     },
     "execution_count": 47,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "categorical_vecs"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 48,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Including all features for model building\n",
    "\n",
    "all_features= continuous_features + categorical_vecs"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 49,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['no_of_projects',\n",
       " 'salary',\n",
       " 'last_performance_ratingsVec',\n",
       " 'dept_nameVec',\n",
       " 'left_orgVec',\n",
       " 'titleVec']"
      ]
     },
     "execution_count": 49,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "all_features"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 50,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Creating the vector of all predictors\n",
    "assembler = VectorAssembler( inputCols = all_features, outputCol = \"features\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 51,
   "metadata": {},
   "outputs": [],
   "source": [
    "df1 = assembler.transform(df1)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 53,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Setting the target variables - left\n",
    "df1 = df1.withColumn( \"label\", df1.left_org.cast( 'int' ) )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 54,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+-----+\n",
      "|            features|label|\n",
      "+--------------------+-----+\n",
      "|(21,[0,1,3,14,15]...|    0|\n",
      "|(21,[0,1,4,7,14,1...|    0|\n",
      "|(21,[0,1,3,7,14,1...|    0|\n",
      "|(21,[0,1,4,13,14,...|    0|\n",
      "|(21,[0,1,2,12,14,...|    0|\n",
      "+--------------------+-----+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df1.select( \"features\", \"label\" ).show( 5 )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 55,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Split the dataset\n",
    "train_df, test_df = df1.randomSplit( [0.7, 0.3], seed = 42 )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 56,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Train Linear Regression Model\n",
    "from pyspark.ml.regression import LinearRegression"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 57,
   "metadata": {},
   "outputs": [],
   "source": [
    "linreg = LinearRegression(maxIter=500, regParam=0.0)\n",
    "lm = linreg.fit( train_df )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 58,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "0.9999999999991521"
      ]
     },
     "execution_count": 58,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Intercept and coefficients\n",
    "lm.intercept"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 59,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "DenseVector([-0.0, -0.0, 0.0, 0.0, 0.0, 0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])"
      ]
     },
     "execution_count": 59,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "lm.coefficients"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 60,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+-----+--------------------+\n",
      "|            features|label|          prediction|\n",
      "+--------------------+-----+--------------------+\n",
      "|(21,[0,1,2,8,14,1...|    0|-1.11022302462515...|\n",
      "|(21,[0,1,4,7,14,1...|    0|5.551115123125783...|\n",
      "|(21,[0,1,3,7,14,1...|    0|3.330669073875469...|\n",
      "|(21,[0,1,4,13,14,...|    0|-1.11022302462515...|\n",
      "|(21,[0,1,4,8,14,1...|    0|-1.11022302462515...|\n",
      "+--------------------+-----+--------------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#Making predictions on test data\n",
    "y_pred  =  lm.transform( test_df )\n",
    "y_pred.select( 'features', 'label', 'prediction' ).show( 5 )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 62,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Metrics to check the accuracy of model (goodness of fit)\n",
    "#Calculating RMSE and R-Squared\n",
    "from pyspark.ml.evaluation import RegressionEvaluator"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 63,
   "metadata": {},
   "outputs": [],
   "source": [
    "rmse_evaluator = RegressionEvaluator(labelCol=\"label\",\n",
    "                            predictionCol=\"prediction\",\n",
    "                            metricName=\"rmse\" )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 64,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "6.6320290966517706e-15"
      ]
     },
     "execution_count": 64,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "lm_rmse = rmse_evaluator.evaluate( y_pred )\n",
    "lm_rmse"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 65,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "1.0"
      ]
     },
     "execution_count": 65,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "r2_evaluator = RegressionEvaluator(labelCol=\"label\",\n",
    "                            predictionCol=\"prediction\",\n",
    "                            metricName=\"r2\" )\n",
    "lm_r2 = r2_evaluator.evaluate( y_pred )\n",
    "lm_r2"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 66,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler\n",
    "from pyspark.ml import Pipeline\n",
    "from pyspark.ml.regression import LinearRegression"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Create indexers for the categorical features"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 68,
   "metadata": {},
   "outputs": [],
   "source": [
    "\n",
    "indexers = [StringIndexer(inputCol=c, outputCol=\"{}_idx\".format(c)) for c in categorical_features]"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## encode the categorical features"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 69,
   "metadata": {},
   "outputs": [],
   "source": [
    "encoders = [ OneHotEncoder(\n",
    "      inputCol=idx.getOutputCol(),\n",
    "      outputCol=\"{0}_enc\".format(idx.getOutputCol())) for idx in indexers]"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Create vectors for all features categorical and continuous\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 70,
   "metadata": {},
   "outputs": [],
   "source": [
    "assembler = VectorAssembler(\n",
    "  inputCols=[enc.getOutputCol() for enc in encoders] + continuous_features,\n",
    "  outputCol=\"features\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Initialize the linear model"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 72,
   "metadata": {},
   "outputs": [],
   "source": [
    "lrModel = LinearRegression( maxIter = 10 )"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Create the pipeline with sequence of activities"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 74,
   "metadata": {},
   "outputs": [],
   "source": [
    "pipeline = Pipeline( stages= [indexers, encoders, assembler, lrModel ])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
