import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
import numpy as np

# Intermediate Queries
# Connect to the MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Mysql123',
    database='ecommerce'
)
cur = db.cursor()

#Advanced Queries
#1. Calculate the moving average of order values for each customer over their order history.
query = """select customer_id, order_purchase_timestamp,payment, avg(payment)
over(partition by customer_id order by order_purchase_timestamp 
rows between 2 preceding and current row) as mov_avg
from 
(select orders.customer_id, orders.order_purchase_timestamp,
payments.payment_value as payment
from payments join orders
on payments.order_id = orders.order_id) as a
"""
cur.execute(query)
data = cur.fetchall()
#df = pd.DataFrame(data)
#print(df)

#2. Calculate the cumulative sales per month for each year.
query = """select years, months, payment, sum(payment)
over(order by years,months) cumulative_sales from 
(select year(order_purchase_timestamp) as years,
monthname(order_purchase_timestamp) as months,
round(sum(payments.payment_value),2) as payment from orders join payments 
on orders.order_id = payments.order_id
group by years,months
order by years,months) as a
"""                                                                          
cur.execute(query)                                                           
data = cur.fetchall()                                                        
#df = pd.DataFrame(data)
#print(df)

#3. Calculate the year-over-year growth rate of total sales.
query = """with a as (                  
select year(order_purchase_timestamp) as years,                                                    
round(sum(payments.payment_value),2) as payment from orders join payments
on orders.order_id = payments.order_id                                   
group by years                                                    
order by years )
select years, ((payment - lag(payment,1) over(order by years))/
lag(payment,1) over(order by years))* 100 from a                                            
"""
#select years, payment, lag(payment,1) over(order by years) previous_year from a
cur.execute(query)                                                       
data = cur.fetchall()                                                    
#df = pd.DataFrame(data, columns=["years", "yoy % growth"])
#print(df)

#4. Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.

query = """
 WITH first_orders AS (
     SELECT
         customer_id,
         MIN(order_purchase_timestamp) AS first_order_date
     FROM
         orders
     GROUP BY
         customer_id
 ),
 
 next_orders AS (
     SELECT
         o.customer_id,
         MIN(o.order_purchase_timestamp) AS next_order_date
     FROM
         orders o
         JOIN first_orders f ON o.customer_id = f.customer_id
     WHERE
         o.order_purchase_timestamp > f.first_order_date
         AND o.order_purchase_timestamp <= DATE_ADD(f.first_order_date, INTERVAL 6 MONTH)
     GROUP BY
         o.customer_id
 )
 
 SELECT
     COUNT(DISTINCT next_orders.customer_id) * 100.0 / COUNT(DISTINCT first_orders.customer_id) AS retention_rate_percentage
 FROM
     first_orders
     LEFT JOIN next_orders ON first_orders.customer_id = next_orders.customer_id
"""
cur.execute(query)
data = cur.fetchall()
#print(data)

#5. Identify the top 3 customers who spent the most money in each year.
query = """
select years,customer_id,payment,d_rank
from
(
select year(orders.order_purchase_timestamp) years,
orders.customer_id,
sum(payments.payment_value) payment,
dense_rank() over(partition by year(orders.order_purchase_timestamp)
order by sum(payments.payment_value) desc)d_rank
from orders join payments
on payments.order_id = orders.order_id
group by year(orders.order_purchase_timestamp),
orders.customer_id) as a
where d_rank <= 3
"""
cur.execute(query)
data = cur.fetchall()
print(data)          
df = pd.DataFrame(data, columns=["years", "id", "payment", "rank"])
sns.barplot(x = "id",y = "payment", data = df, hue = "years")
plt.xticks(rotation = 90)
plt.show()
