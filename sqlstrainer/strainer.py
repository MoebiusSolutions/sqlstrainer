"""Strainer builds a filter path for a mapper/model

.. autoclass:: Strainer
    :members:

"""
from functools import wraps
from sqlstrainer.mapper import DBMap

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'


def _any_to_many(v):
    if isinstance(v, basestring):
        yield v
        return
    try:
        for value in iter(v):
            yield value
    except TypeError:
        yield v


class StrainTarget(object):
    """data class

    If you have a User model and an Address model with a home relationship and a work relationship
    and you wish to search the street field for either you create a StrainTarget with two routes.

    """

    def __init__(self, name, routes):
        """data class

        `routes` is a list of paths. Each path must terminate at the same model

        :param routes: a list of paths
        """

        self.name = name
        self.routes = routes


class Strainer(object):
    """Strainer is ...

    >>> strainer = Strainer(User)
    """
    _strict = False

    def __init__(self, base, dbmap=None, strict_mode=False):
        """
        :param base: base entity - all joins originate from here
        :type base: Declarative or Mapper
        :param dbmap: DBMap (if empty, a default DBMap will be created)
        :type dbmap: :class:`sqlstrainer.mapper.DBMap`
        :param strict_mode: Raises errors when true, skips when false
        :type strict_mode: bool
        """

        if dbmap is None:
            dbmap = DBMap()
        self.dbmap = dbmap
        self.mapper = dbmap.to_mapper(base)
        self.groups = {}
        self.hidden = set()
        self.options = {}
        self._strict = strict_mode

    @property
    def strict(self):
        return self._strict

    def hide(self, obj):
        mapper = self.dbmap.to_mapper(obj)
        self.hidden.add(mapper)

    def unhide(self, obj):
        mapper = self.dbmap.to_mapper(obj)
        self.hidden.remove(mapper)

    def add_path(self, *path):
        path = list(path)
        destination = path[-1]
        path.insert(0, self.mapper)
        self.routes[destination] = self.dbmap.join_path(path)

    def add_clause(self, obj, clause):
        mapper = self.dbmap.to_mapper(obj)
        self.options[mapper] = clause

    def add_value(self, data):
        raise NotImplemented

    def strain(self, data):
        """builds the filter for the given set of data

        data format::

            [
             { 'name': 'column_name1', 'filter': 'startswith', 'value': ['a','b'] },
             { 'name': 'column_name2', 'filter': 'less_than', 'value': 37 }
            ]

        `value` can be either a single value or multiple values.
        By default, each value will be filtered by logical OR
        separate columns will be filtered by logical AND


        :param data: list of data to filter on
        :raises Error: when strict mode is enabled
        """


        for item in data:
            try:
                name = item['name']
                column = self.dbmap[name]
                filter_name = item.get('filter', 'contains')
                column_filter = self.filters[filter_name]
                v = item['value']
                values = list(_any_to_many(v))


            except KeyError as e:
                if self._strict:
                    raise e
                else:
                    continue

    def apply(self, query):
        return query.filter(*self.filter).join(*self.join)


class StrainerMixin(object):

    def strain(self, query, data):
        strainer = Strainer(self.__class__)
        strainer.strain(data)
        return strainer.apply(query)


# class StrainBeMeta(type):
#     _registry = {}
#
#     def __new__(mcs, name, bases, dct):
#         o = super(StrainBeMeta, mcs).__new__(mcs, name, bases, dct)
#         StrainBeMeta._registry[name] = o
#         return o
#
#
# class ColumnStrainer(object):
#     """
#
#     """
#     __metaclass__ = StrainBeMeta
#
#     def __init__(self, column):
#         pass

class ColumnStrainerFactory(object):

    @classmethod
    def create(cls, column_type='*'):
        if column_type == '*':
            default = True


#class TextStrainer(ColumnStrainer):
class TextStrainer(object):
    """

    """
    dict((('strartswith', lambda x, y: x.ilike('{0}%'.format(y))),
         ('endswith', lambda x, y: x.ilike('%{0}'.format(y))),
         ('like', lambda x, y: x.ilike('%{0}%'.format(y))),
         ('not like', lambda x, y: x.ilike('%{0}%'.format(y))),
         ('is', lambda x, y: x.is_(y)),
         ('is not', lambda x, y: x.isnot(y))))


registry = {}


class TypeStrainer(object):
    _column_type = None
    _actions = None

    def __init__(self, column_type, actions):
        self._column_type = column_type
        self._actions = actions

    def strain(self, action, *args):
        return self._actions[action](*args)

    @property
    def column_type(self):
        return self._column_type

    @property
    def actions(self):
        return self._actions
    #filter_for('all')
    def contains(self):
        return False

#DefaultStrainer = TypeStrainer('all', [DefaultStrainer.contains, DefaultStrainer.notcontains])


def filter_for(data_type, action=None):
    """Create decorator

    :param data_type: column type such as string, int,
    :type type: str
    :return: decorator
    """

    def decorator(func):
        name = action if action is not None else func.func_name
        registry.setdefault(data_type, {})[name] = func

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator


