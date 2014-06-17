"""Strainer builds a filter path for a mapper/model

.. autoclass:: Strainer
    :members:

"""
from sqlstrainer.mapper import DBMap

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'



class Strainer(object):
    """Strainer is ...

    >>> strainer = Strainer(User)
    """

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
        self.routes = {}
        self.hidden = set()
        self.options = {}
        self.strict = strict_mode

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
        """

        for item in data:
            name = item.get('name')
            if not name:
                if self.strict:
                    raise AttributeError('name attribute missing')
                else:
                    continue
            column = self.dbmap.get(name)
            if not column:
                if self.strict:
                    raise AttributeError('Unknown column name: "{0}"'.format(name))
                else:
                    continue
            filter = item.get('filter')
            if not filter:
                if self.strict:
                    raise AttributeError('filter attribute missing')
                else:
                    continue
            value = item.get('value')
            if isinstance(value, basestring):
                value = [value]
            try:
                _ = iter(value)
            except TypeError:
                value = [value]

                            
            """lookup name"""
            """iterate values"""
            """execute filter"""
            """build join list"""

    def apply(self, data):
        raise NotImplemented

class StrainerMixin(object):

    def strain(self, query, data):
        strainer = Strainer(self.__class__)
        strainer.strain(data)
        return strainer.apply(query)

column_handlers = {}


class ColumnStrainer(object):
    """

    """

    def __init__(self, column):
        self.handler = column_handlers.get(column.type, column_handlers['DEFAULT'])


class TextStrainer(ColumnStrainer):
    """

    """
    dict((('strartswith', lambda x, y: x.ilike('{0}%'.format(y))),
         ('endswith', lambda x, y: x.ilike('%{0}'.format(y))),
         ('like', lambda x, y: x.ilike('%{0}%'.format(y))),
         ('not like', lambda x, y: x.ilike('%{0}%'.format(y))),
         ('is', lambda x, y: x.is_(y)),
         ('is not', lambda x, y: x.isnot(y))))
