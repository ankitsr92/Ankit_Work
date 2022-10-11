--Create a Parquet File Format

create file format PARQUET_Format
    type =  PARQUET;

--Create Stage
create or replace stage challenge_2_stage url='s3://frostyfridaychallenges/challenge_2/';

--List Stage
list @challenge_2_stage;


--Infer Schema
select *
  from table(
    infer_schema(
      location=>'@challenge_2_stage'
      , file_format=>'PARQUET_Format'
      )
    );

--Generate Column Description
select generate_column_description(array_agg(object_construct(*)), 'table') as columns
  from table (
    infer_schema(
      location=>'@challenge_2_stage',
      file_format=>'PARQUET_Format'
    )
  );

--Create a new table to hold Parquet data
CREATE or replace TABLE PARQUET_DATA
(
"employee_id" NUMBER(38, 0),
"first_name" TEXT,
"last_name" TEXT,
"email" TEXT,
"street_num" NUMBER(38, 0),
"street_name" TEXT,
"city" TEXT,
"postcode" TEXT,
"country" TEXT,
"country_code" TEXT,
"time_zone" TEXT,
"payroll_iban" TEXT,
"dept" TEXT,
"job_title" TEXT,
"education" TEXT,
"title" TEXT,
"suffix" TEXT
);


--Copy into table

COPY INTO PARQUET_DATA
FROM @challenge_2_stage
FILE_FORMAT='PARQUET_Format'
MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';


--Validate data
SELECT * FROM PARQUET_DATA;


--Create a custom view
CREATE VIEW PARQUET_DATA_VW AS SELECT "employee_id","dept","job_title" from PARQUET_DATA;


--Create stream on view
CREATE STREAM PARQUET_DATA_VW_STREAM ON VIEW PARQUET_DATA_VW;

UPDATE PARQUET_DATA SET "country" = 'Japan' WHERE "employee_id" = 8;
UPDATE PARQUET_DATA SET "last_name" = 'Forester' WHERE "employee_id" = 22;
UPDATE PARQUET_DATA SET "dept" = 'Marketing' WHERE "employee_id" = 25;
UPDATE PARQUET_DATA SET "title" = 'Ms' WHERE "employee_id" = 32;
UPDATE PARQUET_DATA SET "job_title" = 'Senior Financial Analyst' WHERE "employee_id" = 68;


--Verify Stream Data
SELECT * FROM PARQUET_DATA_VW_STREAM;