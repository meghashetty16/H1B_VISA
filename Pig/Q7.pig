h1b= load '/user/hive/warehouse/h1bproject.db/h1b' using PigStorage('\t') as
(s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,time_position:chararray,wage:int,year:chararray,work_site:chararray,longitute:double,latitute:double);

--describe h1b;

--dump h1b;

grouph1b = GROUP h1b by year;

--describe grouph1b;

--dump grouph1b;

reduceh1b= foreach grouph1b generate flatten(group),COUNT(h1b.case_status)as count;

--describe reduceh1b;

dump reduceh1b;


