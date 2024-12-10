{{ config(materialized = 'view', schema = 'Reporting')}}
{% set linenos = get_linenos() %}
select
orderid,
{% for lno in linenos %}
sum(case when lineno = {{lno}} then LINESALEAMOUNT end) as lineno{{lno}}_sales,
{% endfor %}

--sum(case when lineno = 2 then LINESALEAMOUNT end) as lineno2_sales,
--sum(case when lineno = 3 then LINESALEAMOUNT end) as lineno3_sales,
sum(LINESALEAMOUNT) as total_sales
from {{ ref('fct_orders') }}
group by 1
