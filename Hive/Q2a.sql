use h1bproject;
drop  table h1b2a;
create table h1b2a (year string,worksite string,count int )
row format delimited
fields terminated by '\t'
stored as textfile;

INSERT OVERWRITE table h1b2a select year,split(worksite,'[,]')[1] as state, count(split(worksite,'[,]')[1]) as job_cnt from h1b where job_title LIKE '%DATA ENGINEER%' group by split(worksite,'[,]')[1],year order by job_cnt;

--LOAD DATA INPATH '/user/hive/warehouse/h1bproject.db/2apartialout' OVERWRITE INTO TABLE h1b2a;
select year ,max(struct(count,worksite)).col1 as name1,max(struct(count,worksite)).col2 as count1 from h1b2a group by year;


