drop table ufodata;

Create table ufodata(sighted string,reported string,Sighting_location string,shape string,duration string,description string comment 'free text description')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

load data inpath '/tmp/ufodata.txt' overwrite into table ufodata;
