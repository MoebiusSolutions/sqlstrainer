"""Matcher contains a dictionary of action handlers to match data based on column type.

Each column type has a list of action handlers which take a column and data.

.. code::

    matchers = {
        sa.Numeric: {
            'lt': lambda c, d: c < d,
            'gt': lambda c, d: c > d,
        # ...
        },
    #    ...
    }


"""
from inspect import getmro
import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.sqltypes import NullType

from functools import wraps


__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

# """
# For all lambdas:
# c = Column:
# d = Data
# """

_default = {
    'contains': lambda c, d: sa.cast(c, sa.String).like('%{0}%'.format(str(d))),
    'notcontains': lambda c, d: sa.not_(sa.cast(c, sa.String).like('%{0}%'.format(str(d)))),
    'is': lambda c, d: c == d,
    'isnot': lambda c, d: c != d,
    'empty': lambda c, d: c.is_(c, None),
    'notempty': lambda c, d: c.isnot(c, None),
}

_bool = {
    'is': lambda c, d: c,
    'isnot': lambda c, d: sa.not_(c),
    'empty': lambda c, d: c.is_(c, None),
    'notempty': lambda c, d: c.isnot(c, None),
}

_numeric = {
    'lt': lambda c, d: c < d,
    'gt': lambda c, d: c > d,
    'le': lambda c, d: c <= d,
    'ge': lambda c, d: c >= d,
    'eq': lambda c, d: c == d,
    'ne': lambda c, d: c != d,
    'ibound': lambda c, d: sa.and_(c >= min(*d), c <= max(*d)),
    'xbound': lambda c, d: sa.and_(c > min(*d), c < max(*d)),
    'is': lambda c, d: c == d,
    'isnot': lambda c, d: c != d,
    'empty': lambda c, d: c.is_(c, None),
    'notempty': lambda c, d: c.isnot(c, None),
}
_date = dict(**_numeric)
_date.update({

})

_datetime = dict(**_numeric)
_datetime.update({

})

_time = dict(**_numeric)
_time.update({

})

_string = {
    'contains': lambda c, d: c.ilike('%{0}%'.format(d)),
    'notcontains': lambda c, d: sa.not_(c.ilike('%{0}%'.format(d))),
    'empty': lambda c, d: sa.or_(c.is_(c, None), c == ''),
    'notempty': lambda c, d: sa.and_(c.isnot(c, None), c != ''),
}
_string.update(_bool)


matchers = {
    sa.Boolean: _bool,
    ClauseElement: _bool,
    NullType: _bool,

    sa.String: _string,

    sa.Numeric: _numeric,
    sa.Integer: _numeric,
    sa.Interval: _numeric,

    sa.Date: _date,
    sa.DateTime: _datetime,
    sa.Time: _time,

}


def column_matcher(column=None, action='contains'):
    """matches a column to and action to a callable

    :param column: SQLAlchemy Column or hybrid_property
    :raises:
    """

    match_type = None
    if column is not None:
        for col_type in getmro(type(column.type)):
            if col_type in matchers:
                match_type = matchers[col_type]
                break

    if match_type is None:
        match_type = _default

    return match_type[action]


def filter_for(data_type, action=None):
    """Create decorator

    :param data_type: column type such as sa.String, sa.Integer...
    :type data_type: str
    :return: decorator
    """

    def decorator(func):
        name = action if action is not None else func.func_name
        matchers.setdefault(data_type, {})[name] = func

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator

