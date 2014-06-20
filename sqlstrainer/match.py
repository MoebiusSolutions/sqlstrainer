"""Matcher

.. code::

    Matcher = {
        sa.Numeric: _numeric,
        sa.Integer: _numeric,
        sa.String: _string,
        sa.Boolean: _numeric,
        sa.Date: _numeric,
        sa.DateTime: _numeric,
        sa.Time: _numeric,
        sa.Interval: _numeric
    }

"""

import sqlalchemy as sa


__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

# """
# c = Column
# d = Data
# """

_numeric = {
    'lt': lambda c, d: c < d,
    'gt': lambda c, d: c > d,
}
_string = {
    'contains': lambda c, d: c.like('%{0}%'.format(d)),
    'notcontains': lambda c, d: sa.not_(c.like('%{0}%'.format(d))),
}
_base = {
    'contains': lambda c, d: sa.cast(c, sa.String).like('%{0}%'.format(str(d))),
    'notcontains': lambda c, d: sa.not_(sa.cast(c, sa.String).like('%{0}%'.format(str(d)))),
    'is': lambda c, d: c == d,
    'isnot': lambda c, d: c != d,
}

Matcher = {
    sa.Numeric: _numeric,
    sa.Integer: _numeric,
    sa.String: _string,
    sa.Boolean: _numeric,
    sa.Date: _numeric,
    sa.DateTime: _numeric,
    sa.Time: _numeric,
    sa.Interval: _numeric
}
