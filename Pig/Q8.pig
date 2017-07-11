--8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order.


h1b= load '/user/hive/warehouse/h1bproject.db/h1b' using PigStorage('\t') as
(s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,time_position:chararray,wage:int,year:chararray,work_site:chararray,longitute:double,latitute:double);

--describe h1b;

--dump h1b;

filterh1b =FILTER h1b by time_position matches 'Y';

--dump filterh1b;

grouph1b = GROUP filterh1b by (year,job_title);

--describe grouph1b;

--dump grouph1b;


reduceh1b= foreach grouph1b generate flatten(group),AVG(filterh1b.wage)as avg;

--describe reduceh1b;

--dump reduceh1b;

orderh1b = ORDER reduceh1b by avg desc;

--describe orderh1b;

--dump orderh1b;

top50= limit orderh1b 50;

dump top50;
