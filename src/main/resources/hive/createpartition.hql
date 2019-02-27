Create table partufo(sighted string,reported string,Sighting_location string,shape string)
partitioned by (year string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

