import snowflake.snowpark.functions as F
import pandas as pd
import holidays 
def avgsale(x,y):
       return y/x

def is_holiday (date_col):
    french_holidays = holidays.France()
    is_holiday = (date_col in french_holidays)
    return is_holiday
    
def model(dbt, session):

    dbt.config(materialized = 'table', schema = 'reporting', packages = ["holidays"])
     
    dim_customers_df = dbt.ref('dim_customers')
 
    fct_orders_df = dbt.ref('fct_orders')
 
    customer_orders_df = (
                            fct_orders_df
                            .group_by('customerid')
                            .agg
                            (
                            F.min(F.col('orderdate')).alias('first_orderdate'),
                            F.max(F.col('orderdate')).alias('recent_orderdate'),
                            F.count(F.col('orderid')).alias('total_orders'),
                            F.sum(F.col('Linesaleamount')).alias('total_sales'),
                            F.avg(F.col('margin')).alias('avg_margin')
                            )
                        )
    final_df = (
                    dim_customers_df
                    .join(customer_orders_df, customer_orders_df.customerid == dim_customers_df.customerid, 'left')
                    .select(
                    dim_customers_df.companyname.alias('companyanme'),
                    dim_customers_df.contactname.alias('contactname'),
                    customer_orders_df.first_orderdate.alias('first_orderdate'),
                    customer_orders_df.recent_orderdate.alias('recent_orderdate'),
                    customer_orders_df.total_orders.alias('total_orders'),
                    customer_orders_df.total_sales.alias('total_sales'),
                    customer_orders_df.avg_margin.alias('avg_margin')
                    )
                )
   
    final_df = final_df.withColumn("avg_salesvalue", avgsale(final_df["total_orders"], final_df["total_sales"]))
    
    final_df = final_df.filter(F.col("first_orderdate").isNotNull())

    final_df = final_df.to_pandas()

    final_df ["IS_FIRST_ORDERDATE_HOLIDAY"] = final_df["FIRST_ORDERDATE"].apply(is_holiday)

    return final_df