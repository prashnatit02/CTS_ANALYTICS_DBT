{{ config(materialized = 'view', schema = 'Reporting')}}

select c.companyname,c.contactname,count(f.orderid) as total_orders 
, sum(f.linesaleamount) as total_sales, avg(f.margin) as avg_margin from
{{ref('fct_orders')}} as f
inner join 
{{ref('dim_customers')}} as c
on c.customerid = f.customerid
group by c.companyname,c.contactname
order by total_sales desc