__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

from functools import wraps


def viewable(**info):
    """Create decorator

    :param info: same as column info but for @hybrid_property
    :type info: kwargs
    :return: decorator
    """

    def decorator(func):
        setattr(func, '_info', info)
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
