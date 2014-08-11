"""Views

Each core model in an app will have 1 or more views of its data.

This module handles both configuring which columns you define as viewable
as well as selecting which are actually part of the view (query).

In any app where you want dynamic selection of viewed columns,
you will need to define viewable columns.

By default:

 * all table columns are viewable.
 * all hybrid properties are hidden
 * no columns are selected for viewing
 * Label generated using `str.title()`

@viewable
class MyModel(Model):
    # viewable (not selected by default)
    first_name = Column(String)
    # hidden (not selectable)
    noseeum = Column(String, default='Secret', info={'hidden': True})


    @hybrid_property
    def filterable_only(self):
        return self.a - self.b

    @hybrid_property
    @viewable(label='Full Name')
    def full_name(self):
        return self.first_name + " " + self.last_name

-----------
NOTES: viewable uses decorator
view - list from dev
view - default = model
handles aggregate.

Aggregate links to separate tables are mutually exclusive,
 - meaning if you aggregate a relationship you cannot also get individual row data from that relationship

view - can pass options



MODEL:
  default viewable

From a VIEW:
   define View type [aggregate|nested|distinct] (BASE MODEL) [distinct=?=aggregate]

   filters -> traversal (should be universal/view independent) [but configurable just in case]
   filters -> special case
        - aggregate (use having)
        - not in - use outerjoin, id IS NULL [for performance]



"""
import inspect
from sqlalchemy.ext.hybrid import hybrid_property

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

from functools import wraps

def _viewable(cls):
    yield 'items'

def viewable(**info):
    """Create decorator

    :param info: same as column info but for @hybrid_property
    :type info: kwargs
    :return: decorator
    """

    def decorator(obj):
        if inspect.isclass(obj):
            oldget = obj.__getattribute__
            def newget(cls, item):
                if item == 'viewable':
                    return _viewable(cls)
                return oldget(cls, item)
            obj.__getattribute__ = newget
            return obj

        obj._info = info
        func = obj

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator


""" possible userview

class UserView(IDModel):
    owner_id = db.Column(db.Integer, ForeignKey=Person.id)
    # separate from user, figure out Privileges
    # visible_to_roles = db.relationship()

    prefix = db.Column(db.String, nullable=False)
    '''url prefix'''
    name = db.Column(db.String, nullable=False)
    '''Display Name'''
    columns = db.Column(postgresql.ARRAY(db.String))
    group_by = db.Column(postgresql.ARRAY(db.String)) # ????
    order_by = db.Column(postgresql.ARRAY(db.String))

#    filter
    owner = db.relationship(Person, backref='views')
"""


""" list of viewable columns

>>> cn, invalid_findings, ...

key: 'invalid_findings'
label: 'Invalid Findings',
column: Device.invalid_findings (could always match, be hybrid or column)

decorator.. gives


@viewable
@filterable
class Device(IDModel):
    ...

    cn = db.Column(db.String, info={'label': 'Device Name'})
    active = db.Column(db.Boolean, info={'hidden': True})

    @viewable(label='ACAS Compliant')
    @hybrid_property
    def compliant(self):
        return self.active & self.valid

"""
device_view = {
    'base': 'device',
    'relatives': ['device.findings', 'finding.plugin', 'device.users'],
    'columns': [
        ('cn', 'column2', 'plugin.risk', '...')
    ]
}

"""
def total_invalid():
    return db.sum(db.case((Plugin.risk == Plugin.risk_map['Invalid'], 1), else_=0)).label('test')

{'label': 'Text', 'value': db.func.count('*'), 'aggregate': Plugin}

"""
