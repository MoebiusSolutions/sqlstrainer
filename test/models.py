""" test models

http://www.databaseanswers.org/data_models/customers_inventory_and_pos/index.htm

"""
import random
import re
from faker import Factory
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, Integer, String, ForeignKey, Date, DECIMAL, func, DateTime
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base, declared_attr

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

Base = declarative_base()


class EqMixin(object):
    """Compare and hash objects by custom values."""

    def compare_value(self):
        raise NotImplementedError

    def __eq__(self, other):
        """Instances of same class with equal compare values are equal."""

        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.compare_value() == other.compare_value()

    def __ne__(self, other):
        eq = self.__eq__(other)
        if eq is NotImplemented:
            return eq
        return not eq

    def __hash__(self):
        """Composite hash of class and compare value."""

        return hash(self.__class__) ^ hash(self.compare_value())


_cc_re = re.compile(r'([A-Z]+)(?=[a-z0-9])')
"""Camel Case regex"""


class Model(EqMixin, Base):
    """Model base that provides automatic table name and other convenient
    defaults."""

    __abstract__ = True

    @declared_attr
    def __tablename__(cls):
        """If __tablename__ is not otherwise set, split the class name as camelcase."""

        def _join(match):
            word = match.group()
            if len(word) > 1:
                return ('_{0}_{1}'.format(word[:-1], word[-1])).lower()
            return '_' + word.lower()

        return _cc_re.sub(_join, cls.__name__).lstrip('_')

    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)

    def __repr__(self):
        """Show class name and instance string."""

        return '<{0} {1}>'.format(self.__class__.__name__, self)

    def compare_value(self):
        """Use primary key(s) for equality."""

        return self._sa_instance_state.identity_key


class Parent(Model):
    parent_id = Column(Integer, autoincrement=True, primary_key=True)
    first_name = Column(String)
    middle_name = Column(String)
    last_name = Column(String)
    dob = Column(Date)
    gender = Column(String)
    details = Column(String)


class Customer(Model):
    customer_id = Column(Integer, autoincrement=True, primary_key=True)
    parent_id = Column(Integer, ForeignKey(Parent.parent_id))
    first_name = Column(String)
    middle_name = Column(String)
    last_name = Column(String)
    dob = Column(Date)
    gender = Column(String)
    current_balance = Column(Integer, nullable=False, default=0)
    date_of_last_deposit = Column(Date)
    amount_of_last_deposit = Column(Integer, nullable=False, default=0)
    details = Column(String)
    parent = orm.relationship(Parent, backref='children')


class Order(Model):
    order_id = Column(Integer, autoincrement=True, primary_key=True)
    customer_id = Column(Integer, ForeignKey(Customer.customer_id))
    order_date = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    derived_order_value = Column(Integer, nullable=False, default=0)
    details = Column(String)
    customer = orm.relationship(Customer, backref='orders')


class UnitOfMeasure(Model):
    uom_code = Column(String(2), primary_key=True)
    description = Column(String, nullable=False)


class Product(Model):
    product_id = Column(Integer, autoincrement=True, primary_key=True)
    uom_code = Column(String(2), ForeignKey(UnitOfMeasure.uom_code), nullable=False)
    price = Column(Integer, nullable=False, default=0)
    details = Column(String)
    uom = orm.relationship(UnitOfMeasure, uselist=False)


class OrderProduct(Model):
    order_id = Column(Integer, ForeignKey(Order.order_id), primary_key=True)
    product_id = Column(Integer, ForeignKey(Product.product_id), primary_key=True)
    quantity_ordered = Column(Integer, nullable=False, default=0)
    order = orm.relationship(Order, backref='product_quantity')
    product = orm.relationship(Product, backref='order_quantity')


Order.products = association_proxy('product_quantity', 'product')
Product.orders = association_proxy('order_quantity', 'order')


class ProductInventory(Model):
    level_id = Column(Integer, autoincrement=True, primary_key=True)
    product_id = Column(Integer, ForeignKey(Product.product_id))
    inventory_date = Column(Date)
    quantity_in_stock = Column(Integer, nullable=False, default=0)
    details = Column(String)
    product = orm.relationship(Product, backref='inventory')


def build_fake_data(session):
    fake = Factory.create()
    codes = dict((
        ('kg', 'Kilogram'),
        ('lb', 'Pound'),
        ('m', 'Meter'),
        ('ea', 'Each')
    ))
    for uom_code, description in codes.iteritems():
        uom = UnitOfMeasure(uom_code=uom_code, description=description)
        session.add(uom)
    session.flush()

    for i in range(100):
        code = random.choice(list(codes.keys()))
        price = random.uniform(0.5, 1000)
        product = Product(uom_code=code, price=price, details=fake.sentence())
        session.add(product)
    session.flush()

    for i in range(10):
        fn = fake.first_name()
        mn = fake.first_name()
        ln = fake.last_name()
        dob = fake.date_time_between(start_date="-100y", end_date="-10y")
        gender = random.choice(['male', 'female'])
        p = Parent(first_name=fn, middle_name=mn, last_name=ln, dob=dob, gender=gender, details=fake.sentence())
        session.add(p)
    session.flush()

    parents = list(pid for pid, in session.query(Parent.parent_id))
    customers = []
    for i in range(100):
        parent = random.choice(parents)
        fn = fake.first_name()
        mn = fake.first_name()
        ln = fake.last_name()
        dob = fake.date_time_between(start_date="-100y", end_date="-10y")
        gender = random.choice(['male', 'female'])
        bal = random.uniform(0, 1000)
        last_deposit = fake.date_time()
        amount = random.uniform(0, 1000)
        c = Customer(parent_id=parent, first_name=fn, middle_name=mn, last_name=ln, dob=dob, gender=gender,
                     current_balance=bal, date_of_last_deposit=last_deposit, amount_of_last_deposit=amount,
                     details=fake.sentence())
        session.add(c)
        customers.append(c)

    session.flush()

    for i in range(1000):
        o = Order()
        o.customer = random.choice(customers)
        o.order_date = fake.date_time()
        o.derived_order_value = random.uniform(0.5, 1000)
        o.details = fake.sentence()
    session.flush()

    products = [pid for pid, in session.query(Product.product_id)]
    for oid, in session.query(Order.order_id):
        for pid in random.sample(products, random.randint(1, 10)):
            op = OrderProduct(order_id=oid, product_id=pid, quantity_ordered=random.randint(1, 5))
            session.add(op)
    session.flush()

    for pid in products:
        for i in range(random.randint(1, 10)):
            inv_date = fake.date_time_between(start_date="-2y", end_date="now")
            pi = ProductInventory(product_id=pid, inventory_date=inv_date,
                                  quantity_in_stock=random.randint(0, 1000), details=fake.sentence())
            session.add(pi)
    session.flush()


def dump(session):
    from sqlstrainer import mapper
    import csv
    outfile = open('dump.csv', 'wb')
    outcsv = csv.writer(outfile)
    dbmap = mapper.DBMap()
    mappers = dbmap._relations.keys()
    for mapper in mappers:
        outcsv.writerow([str(mapper.entity.__name__)])
        columns = [c for c in mapper.columns]
        q = session.query(*columns)
        outcsv.writerow([c.name for c in columns])
        for row in q:
            outcsv.writerow(list(row))
    outfile.close()
