import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
#Basic Queries
# Connect to the MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Mysql123',
    database='ecommerce'
)
cur = db.cursor()
#List all unique cities where customers are located.
query = """select distinct customer_city from customers"""
cur.execute(query)
data = cur.fetchall()
#print(data)

# Count the number of orders placed in 2017
query = """select count(order_id) from ecommerce.orders where year(order_purchase_timestamp) = 2017"""
cur.execute(query)
data = cur.fetchall()
#print(data[0][0])

#Find the total sales per category.
query = """select ecommerce.products.product_category category,
round(sum(ecommerce.payments.payment_value),2) sales
from ecommerce.products join ecommerce.order_items 
on ecommerce.products.product_id = ecommerce.order_items.product_id
join ecommerce.payments
on ecommerce.payments.order_id = ecommerce.order_items.order_id
group by category"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data) # dataframe to use beautify data
df = pd.DataFrame(data,columns = ["Category","Sales"])
#print(df)

#Calculate the percentage of orders that were paid in installments.
query = """select (sum(case when payment_installments >= 1 then 1 else 0 end))/count(*)*100 from payments"""
cur.execute(query)
data = cur.fetchall()
#print(data[0][0])

#Count the number of customers from each state.
query = """SELECT customer_state,count(customer_id) FROM ecommerce.customers group by customer_state"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data,columns = ["state","customer_count"])
print(df)
plt.bar(df["state"],df["customer_count"])
plt.xticks(rotation = 90)
plt.show()