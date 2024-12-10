{{ config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA', 'STAGING_DEV')) }}
 
SELECT
OrderID,
LineNo,
ShipperID,
CustomerID,
ProductID,
EmployeeID,
split_part(ShipmentDate,' ',0)::DATE AS ShipmentDate,
Status
FROM
{{ source('qwt_raw','SHIPMENTS')}}