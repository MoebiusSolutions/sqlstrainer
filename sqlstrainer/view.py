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
from sqlalchemy.orm import ColumnProperty
from sqlalchemy.orm.attributes import InstrumentedAttribute
from functools import wraps

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

_make_label = lambda s: s.replace('_', ' ').title()

def find_viewable(cls, label=None, include=None, exclude=None):
    """
    """
    if exclude is None:
        exclude = set()
    yield cls.__tablename__, label if label else _make_label(cls.__tablename__)

    for supercls in cls.__mro__:
        for key in set(supercls.__dict__).difference(exclude):
            exclude.add(key)
            o = supercls.__dict__[key]
            exclude.add(key)
            info = None
            if isinstance(o, InstrumentedAttribute):
                if isinstance(o.property, ColumnProperty):
                    info = o.info
            elif hasattr(o, 'fget') and hasattr(o.fget, '_info'):
                info = getattr(o.fget, '_info')
            if info is None or info.get('hidden'):
                continue

            lbl = info.get('label')
            if not lbl:
                lbl = _make_label(key)
            yield '{0}.{1}'.format(cls.__tablename__, key), lbl

    for relative in include:
        for key, lbl in relative.viewable:
            yield key, lbl

def viewable(**info):
    """Create decorator

    :param info: same as column info but for @hybrid_property
    :type info: kwargs
    :return: decorator
    """

    def decorator(obj):
        if inspect.isclass(obj):
            mapper = getattr(obj, '__mapper__')
            if not mapper:
                raise TypeError('Expected SQLAlchemy declarative_base() Class')
            obj.viewable = property(lambda: find_viewable(obj, **info))
            return obj

        func = obj
        obj.info = info

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator

