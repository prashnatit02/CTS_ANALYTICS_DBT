{{config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA', 'STAGING_DEV'))}}

select CustomerID,CompanyName,ContactName,City,Country,DivisionID,Address,Fax,Phone,PostalCode,
StateProvince as state 
from 
{{ source('qwt_raw','customers')}}