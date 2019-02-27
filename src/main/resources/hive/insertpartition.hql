set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table partufo partition (year )
select sighted,reported,Sighting_location,shape,
substr(trim(sighted),1,4) from ufodata;

