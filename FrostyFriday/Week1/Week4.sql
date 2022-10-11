--Create Stage

Create Stage JSON_STG url='s3://frostyfridaychallenges/challenge_4/';

--List files on stage
list @JSON_STG;

--JSON File Format
CREATE FILE FORMAT JSON_FORMAT
TYPE='JSON';

--Create Table holding raw data
CREATE TABLE JSON_DATA
(
VAL VARIANT);

--Load data
COPY INTO JSON_DATA
FROM @JSON_STG
FILE_FORMAT='JSON_FORMAT';


--Parse Json
SELECT 
ROW_NUMBER() OVER(ORDER BY M.VALUE:Birth::date asc) as ID,
M.INDEX+1 AS INTER_HOUSE_ID,
VAL:Era::text as Era,
H.VALUE:House::text as House,
M.VALUE:Name::text,
COALESCE(M.VALUE:Nickname[0],M.VALUE:Nickname)::text as NickName_1,
M.VALUE:Nickname[1]::text as NickName_2,
M.VALUE:Nickname[2]::text as NickName_3,
M.VALUE:Birth::date as Birth,
M.VALUE:"Place of Birth"::text as "Place of Birth",
M.VALUE:"Start of Reign"::date AS "Start of Reign",
COALESCE(M.VALUE:"Consort\/Queen Consort"[0],M.VALUE:"Consort\/Queen Consort")::text AS QUEEN_OR_QUEEN_CONSORT_1,
M.VALUE:"Consort\/Queen Consort"[1]::text AS QUEEN_OR_QUEEN_CONSORT_2,
M.VALUE:"Consort\/Queen Consort"[2]::text AS QUEEN_OR_QUEEN_CONSORT_3,
M.VALUE:"End of Reign"::date AS "End of Reign",
M.VALUE:Duration::text AS Duration,
M.VALUE:Death::date as Death,
STRTOK(M.VALUE:"Age at Time of Death",' ',1)::NUMBER AS "Age at Time of Death",
M.VALUE:"Place of Death"::text as "Place of Death",
M.VALUE:"Burial Place"::text as "Burial Place"
FROM JSON_DATA v,
LATERAL FLATTEN(input=>VAL:Houses) H,
LATERAL FLATTEN(input=>H.VALUE:Monarchs) M
;



