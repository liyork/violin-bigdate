insert overwrite directory '/tmp/out'
row format delimited
fields terminated by ','
select t1.sighted,t2.fullname
from ufodata t1 inner join states t2
on t1.reported = t2.refid;
