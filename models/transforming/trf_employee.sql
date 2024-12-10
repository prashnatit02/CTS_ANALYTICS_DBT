{{config (materialized = 'table', schema = 'transforming' )}}

select 
emp.empid,
emp.Lname,emp.fname
,emp.title,emp.hdate,emp.ext
,iff(mgr.fname is null, emp.fname,mgr.fname) as manager,
emp.year_salary,
off.officecity,
off.officecountry
from 
{{ref('stg_employee_off')}} as emp
left join {{ref('stg_employee_off')}} as mgr
on emp.Rto = mgr.empid
inner join {{ref('lkp_offices')}} as off
on emp.office = off.office