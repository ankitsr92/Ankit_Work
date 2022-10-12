-- Create the warehouses
create warehouse if not exists my_xsmall_wh 
    with warehouse_size = XSMALL
    auto_suspend = 120;
    
create warehouse if not exists my_small_wh 
    with warehouse_size = SMALL
    auto_suspend = 120;

-- Create the table
create or replace table test
(
    date_time datetime,
    trans_amount double
);

-- Create the stage with directory
create or replace stage week_10_frosty_stage
    url = 's3://frostyfridaychallenges/challenge_10/',
    directory=(enable=true),
    file_format = (skip_header = 1, type = csv)
    ;

--Refresh Directory
alter stage week_10_frosty_stage refresh;


select * from directory(@week_10_frosty_stage);

-- Create the stored procedure
create or replace procedure dynamic_warehouse_data_load(stage_name string, table_name string)
returns varchar
language javascript
execute as caller
as 
$$

var stg_nm=STAGE_NAME;
var tbl_nm=TABLE_NAME;


var dir_sql="select relative_path,size from directory(@"+stg_nm+")";

var stmt1 = snowflake.createStatement( { sqlText:dir_sql} );

var rs1 = stmt1.execute();

var size = 0;

var file_lst_10kb = [];
var file_lst = [];

while ( rs1.next() )
{

size=rs1.getColumnValue(2);

    if ( size <= 10000 )
    {

    file_lst_10kb.push("'"+rs1.getColumnValue(1)+"'");

    }   
    else
    {
    file_lst.push("'"+rs1.getColumnValue(1)+"'");
    }

}

var wh="use warehouse my_xsmall_wh";

var wh_small="use warehouse my_small_wh";

var wh1 = snowflake.createStatement( { sqlText:wh} );

var wh2 = snowflake.createStatement( { sqlText:wh_small} );

var tmsp = "set tmsp = (select current_time())";

var tmsp_create = snowflake.createStatement ( { sqlText:tmsp});

tmsp_create.execute();

wh1.execute();
var copy_stmt1 = "COPY INTO "+tbl_nm+" FROM  @" +stg_nm+ " FILES = ( " + file_lst_10kb + ")";
var copy_stmt1_create= snowflake.createStatement( { sqlText:copy_stmt1 });
copy_stmt1_create.execute();

wh2.execute();
var copy_stmt2 = "COPY INTO "+tbl_nm+" FROM  @" +stg_nm+ " FILES = ( " + file_lst + ")";
var copy_stmt2_create= snowflake.createStatement( { sqlText:copy_stmt2 });
copy_stmt2_create.execute();

var get_row_count="SELECT SUM(ROW_COUNT) from INFORMATION_SCHEMA.LOAD_HISTORY WHERE TABLE_NAME=\'"+tbl_nm+"\' and LAST_LOAD_TIME>=$tmsp";

var get_row_count_create = snowflake.createStatement( { sqlText:get_row_count});

var rs2 = get_row_count_create.execute();
 
rs2.next();

var row_count=rs2.getColumnValue(1);

return "Rows Load:"+row_count;

$$;


call dynamic_warehouse_data_load('week_10_frosty_stage','TEST');
