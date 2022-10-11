--Create a stage
CREATE OR REPLACE STAGE week3_stg url='s3://frostyfridaychallenges/challenge_3/';

--List stage files
list @week3_stg;

--Create table to load all csv data

create or replace TABLE MY_CSV_FILE (
	file_name text,
    csv_data VARIANT,
	LOAD_TMSP TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE FILE FORMAT VARIANT_FORMAT
	FIELD_DELIMITER = 'NONE',SKIP_HEADER=1
;

--Load using week1 load process
COPY INTO MY_CSV_FILE
FROM 
(SELECT metadata$filename,$1::variant,CURRENT_TIMESTAMP() FROM @week3_stg)
FILE_FORMAT='VARIANT_FORMAT'
;

--Keyword file data
CREATE VIEW KEYWORDS AS
SELECT STRTOK(CSV_DATA,',',1) AS KEYWORDS FROM MY_CSV_FILE where file_name like '%keywords.csv%';


--Get info if keywords data got loaded

SELECT K.KEYWORDS,L.FILE_NAME,L.TABLE_NAME,L.STATUS,L.ROW_COUNT FROM KEYWORDS K
LEFT JOIN INFORMATION_SCHEMA.LOAD_HISTORY L
ON
L.FILE_NAME LIKE CONCAT('%',CONCAT(K.KEYWORDS,'%.csv'))
AND TABLE_NAME='MY_CSV_FILE';

;
SELECT * FROM INFORMATION_SCHEMA.LOAD_HISTORY;
