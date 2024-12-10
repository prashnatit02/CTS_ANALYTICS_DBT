{{config(materialized = 'table' ,schema = env_var('DBT_STAGESCHEMA', 'STAGING_DEV'))}}

select empid ,
Lname ,
fname ,
title ,
hdate ,
office,
ext ,
Rto ,
Year_salary 
from 
{{ source('qwt_raw','emplyee_off')}}