--4)Which top 5 employers file the most petitions each year? - Case Status - ALL

h1b= load '/user/hive/warehouse/h1bproject.db/h1b' using PigStorage('\t') as
(s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,time_position:chararray,wage:int,year:chararray,work_site:chararray,longitute:double,latitute:double);

--describe h1b;

--dump h1b;

year_employer= group h1b by (year,employer_name);

describe year_employer;

--dump year_jobtitle;

foreachyear_employer= foreach year_employer GENERATE flatten(group),COUNT_STAR(h1b.case_status) as count;

--describe foreachyear_jobtitle;

--dump foreachyear_employer;

store foreachyear_employer into '/niit/h1b4' using PigStorage(',');

h1btop10= load '/niit/h1b4/part-r-00000' using PigStorage(',') as (year:chararray,name:chararray,count:int);

--dump h1btop10;

h1bgroup= group h1btop10 by year;

--dump h1bgroup;

top10= foreach h1bgroup {
sort= order h1btop10 by count desc;
top= limit sort 5;
generate flatten(top);
};

dump top10;

store top10 into '/niit/h1b4out' using PigStorage(',');

