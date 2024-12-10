{{
    config(
        materialized = 'table',
        schema = 'transforming'
    )
}}
 
SELECT
SS.ORDERID,
SH.COMPANYNAME,
SS.SHIPMENTDATE,
SS.STATUS,
SS.DBT_VALID_FROM AS VALID_FROM,
SS.DBT_VALID_TO AS VALID_TO
FROM
{{ref('shipments_snapshot')}} as SS
 inner join
 {{ref('lkpshippers')}} as SH on SS.SHIPPERID=SH.SHIPPERID