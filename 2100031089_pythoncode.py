from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:5432@localhost/endsem'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)




class Customers(db.Model):
    __tablename__ = 'customers'
    CustomerID = db.Column(db.Integer, primary_key=True)
    
    
    DateOfBirth = db.Column(db.Date)

class Products(db.Model):
    __tablename__ = 'products'
    ProductID = db.Column(db.Integer, primary_key=True)
    ProductName = db.Column(db.String(50))
    Price = db.Column(db.Float)



class OrderItems(db.Model):
    __tablename__ = 'orderItems'
    OrderItemID = db.Column(db.Integer, primary_key=True)
    OrderID = db.Column(db.Integer, db.ForeignKey('orders.orderID'))
    ProductID = db.Column(db.Integer, db.ForeignKey('products.productID'))
    Quantity = db.Column(db.Integer)



# 1. List all customers.
@app.route('/list_customers')
def list_customers():
    customers = Customers.query.all()
    result = [{'CustomerID': c.CustomerID, 'FirstName': c.FirstName, 'LastName': c.LastName, 'Email': c.Email, 'DateOfBirth': str(c.DateOfBirth)} for c in customers]
    return jsonify(result)

# 2. Find all orders placed in January 2023.
@app.route('/orders_in_january_2023')
def orders_in_january_2023():
    orders = Orders.query.filter(db.extract('year', Orders.OrderData) == 2023, db.extract('month', Orders.OrderData) == 1).all()
    result = [{'OrderID': o.OrderID, 'CustomerID': o.CustomerID, 'OrderDate': str(o.OrderData)} for o in orders]
    return jsonify(result)

# 3. Get the details of each order, including the customer name and email.
@app.route('/order_details')
def order_details():
    order_details = db.session.query(Orders, Customers).join(Customers, Orders.CustomerID == Customers.CustomerID).all()
    result = [{'OrderID': o.OrderID, 'CustomerFirstName': c.FirstName, 'CustomerLastName': c.LastName, 'CustomerEmail': c.Email, 'OrderDate': str(o.OrderDate)} for o, c in order_details]
    return jsonify(result)

# 4. List the products purchased in a specific order (e.g., OrderID = 1).
@app.route('/products_in_order/<int:order_id>')
def products_in_order(order_id):
    products = db.session.query(Products, OrderItems).join(OrderItems, Products.ProductID == OrderItems.ProductID).filter(OrderItems.OrderID == order_id).all()
    result = [{'ProductID': p.ProductID, 'ProductName': p.ProductName, 'Quantity': oi.Quantity} for p, oi in products]
    return jsonify(result)

# 5. Calculate the total amount spent by each customer.
@app.route('/total_spent_by_customer')
def total_spent_by_customer():
    total_spent = db.session.query(Customers.CustomerID, Customers.FirstName, Customers.LastName, Customers.Email, db.func.sum(Products.Price * OrderItems.Quantity).label('TotalSpent')) \
        .join(Orders, Customers.CustomerID == Orders.CustomerID) \
        .join(OrderItems, Orders.OrderID == OrderItems.OrderID) \
        .join(Products, OrderItems.ProductID == Products.ProductID) \
        .group_by(Customers.CustomerID).all()
    result = [{'CustomerID': c.CustomerID, 'FirstName': c.FirstName, 'LastName': c.LastName, 'Email': c.Email, 'TotalSpent': ts.TotalSpent} for c, ts in total_spent]
    return jsonify(result)

# 6. Find the most popular product (the one that has been ordered the most).
@app.route('/most_popular_product')
def most_popular_product():
    most_popular = db.session.query(Products.ProductID, Products.ProductName, db.func.sum(OrderItems.Quantity).label('TotalQuantity')) \
        .join(OrderItems, Products.ProductID == OrderItems.ProductID) \
        .group_by(Products.ProductID, Products.ProductName) \
        .order_by(db.desc(db.func.sum(OrderItems.Quantity))) \
        .first()
    result = {'ProductID': most_popular.ProductID, 'ProductName': most_popular.ProductName, 'TotalQuantity': most_popular.TotalQuantity}
    return jsonify(result)

# 7. Get the total number of orders and the total sales amount for each month in 2023.
@app.route('/monthly_sales_2023')
def monthly_sales_2023():
    monthly_sales = db.session.query(db.func.date_trunc('month', Orders.OrderDate).label('Month'), db.func.count(Orders.OrderID).label('TotalOrders'), db.func.sum(Products.Price * OrderItems.Quantity).label('TotalSales')) \
        .join(OrderItems, Orders.OrderID == OrderItems.OrderID) \
        .join(Products, OrderItems.ProductID == Products.ProductID) \
        .filter(db.extract('year', Orders.OrderDate) == 2023) \
        .group_by(db.func.date_trunc('month', Orders.OrderDate)).all()
    result = [{'Month': str(ms.Month), 'TotalOrders': ms.TotalOrders, 'TotalSales': ms.TotalSales} for ms in monthly_sales]
    return jsonify(result)

# 8. Find customers who have spent more than $1000.
@app.route('/big_spenders')
def big_spenders():
    big_spenders = db.session.query(Customers, db.func.sum(Products.Price * OrderItems.Quantity).label('TotalSpent')) \
        .join(Orders, Customers.CustomerID == Orders.CustomerID) \
        .join(OrderItems, Orders.OrderID == OrderItems.OrderID) \
        .join(Products, OrderItems.ProductID == Products.ProductID) \
        .group_by(Customers.CustomerID) \
        .having(db.func.sum(Products.Price * OrderItems.Quantity) > 1000).all()
    result = [{'CustomerID': c.CustomerID, 'FirstName': c.FirstName, 'LastName': c.LastName, 'Email': c.Email, 'TotalSpent': ts.TotalSpent} for c, ts in big_spenders]
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
