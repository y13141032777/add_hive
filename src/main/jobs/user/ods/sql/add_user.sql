drop database  if exists add_ods_1 cascade;
drop database  if exists add_dim_1 cascade;
drop database  if exists add_dwd_1 cascade;
drop database  if exists add_dws_1 cascade;
drop database  if exists add_dm_1 cascade;

create database  add_dim_1;	
create database  add_ods_1;
create database add_dwd_1;
#create database add_dws_1;



use add_ods_1;
--ods源数据表
create external table  if not exists add_ods_1.ods_log(
    sid String comment  '投放请求id',
    device_num  String comment '设备号',
    release_session String comment '投放会话id' ,
    release_status  String comment  '所处流程状态',
    device_type String comment  '1 android| 2 ios | 9 其他',
    sources string comment '渠道',
    channels string comment '通道',
    exts string comment '扩展信息(参见下面扩展信息描述)',
    ct bigint comment '创建时间'
)
partitioned by (dt string)
stored as parquet
location '/data/add/ods/log/log.77'
;

--load
load data local inpath  "/data/add/ods/log/log.77"  into table add_ods_1.ods_log  partition (dt="${today}");

--dim_维度
use add_dim_1;
create external table if not exists add_dim_1.area(
region_code String comment  "县级代码",
region_code_desc String comment  "县名称",
region_city String comment "城市代码",
region_city_desc String comment "城市名称",
region_province String comment "省级代码",
region_province_desc String comment "省级名称"
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
location '/data/add/dim/area'
;
load data local inpath '/data/add/dim/area.csv' into table add_dim_1.area;

--dwd
use add_dwd_1;
create table add_dwd_1.dwd_user as 
select 
a.*,
region_code_desc as town ,
region_city_desc as city,
region_province_desc as province
from     
(
select 
sid,
device_num,
release_session,
release_status,
device_type,
sources,
channels,
ct ,
get_json_object(exts,"$.area_code")  as  area_code,
floor((unix_timestamp() - unix_timestamp(substring(get_json_object(exts,"$.idcard"),7,8), 'yyyyMMdd'))/60/60/24/365.25) as age
from   add_ods_1.ods_log) a
 left  join add_dim_1.area dim_area on  dim_area.region_code= a.area_code
 ;
 
--union

create table add_dwd_1.dwd_user_age_l_time as 
select  
time_stage,
age_stage,
province, city, town,
count(sid) as num
from(
select  
*,
case  when hour(from_unixtime(cast(ct/1000 as bigint)))>=6 and hour(from_unixtime(cast(ct/1000 as bigint)))>=12  then "上午"
when hour(from_unixtime(cast(ct/1000 as bigint)))>12 or hour(from_unixtime(cast(ct/1000 as bigint)))<18  then "下午"  
 else "晚上" end  as time_stage,
 case when age>40 then 'old' else 'young' end as age_stage
from dwd_user)  a
group by  
a.time_stage,
a.age_stage,
province, city, town
limit 20
;


create database add_dm_1;