"""Strainer builds a filter path for a mapper/model

.. autoclass:: Strainer
    :members:

"""
import itertools
from sqlalchemy import inspect

from sqlstrainer.mapper import DBMap
from sqlstrainer.match import column_matcher
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


class Group(object):
    name = None
    base = None
    _paths = None
    _include = None
    _exclude = None
    flags = None

    def __init__(self, base, name=None):
        self.base = base
        self.name = name if name else getattr(base, '__name__', 'Other')
        self._paths = []
        self._include = []
        self._exclude = []

    @property
    def exclude(self):
        for group in self.all:
            for column in group._exclude:
                yield column

    @property
    def paths(self):
        for group in self.all:
            for path in group._paths:
                yield path

    @property
    def all(self):
        stack = []
        stack.append(self)
        while stack:
            group = stack.pop()
            yield group
            for child in reversed(group._include):
                stack.append(child)


_logical_operations = {
    'any': lambda x, y: x | y,
    'all': lambda x, y: x & y,
}


class Strainer(object):
    """Strainer is ...

    >>> strainer = Strainer(User)
    """
    _strict = False
    _filters = None

    restrictive = True

    def __init__(self, base, dbmap=None, strict=False, all_relatives=False):
        """
        :param base: base entity - all joins originate from here
        :type base: Declarative or Mapper
        :param dbmap: DBMap (if empty, a default DBMap will be created)
        :type dbmap: :class:`sqlstrainer.mapper.DBMap`
        :param strict: Strict Mode : errors raise exceptions, default skip errors
        :type strict: bool
        """

        if dbmap is None:
            dbmap = DBMap()
        self._dbmap = dbmap
        mapper = dbmap.to_mapper(base)
        self._group = Group(mapper)
        if all_relatives:
            relatives = dbmap.all_relatives(mapper)
            for relative, paths in relatives.iteritems():
                shortest_path = sorted(paths, key=lambda sp: len(sp))[0]
                self.group._paths.append(shortest_path)

        self.options = {}
        self._strict = strict
        self._filters = None

    @property
    def columns(self):
        mappers = set()
        exclude_objs = set()
        for group in self.group.all:
            mappers.add(group.base)
            exclude_objs.update(group.exclude)
            for path in group.paths:
                target = self._dbmap.to_mapper(path[-1])
                mappers.add(target)
        exclude = set()
        for obj in exclude_objs:
            if isinstance(obj, basestring):
                if '.' in obj:
                    column = self._dbmap.get(obj)
                    if column is not None:
                        exclude.add(column)
                else:
                    mapper = self._dbmap.get_mapper(obj)
                    for name, column in self._dbmap.columns_of(mapper):
                        exclude.add(column)
            else:
                mapper = self._dbmap.to_mapper(obj)
                if getattr(mapper, 'is_mapper', False):
                    for name, column in self._dbmap.columns_of(mapper):
                        exclude.add(column)

        columns = set()
        for mapper in mappers:
            for name, column in self._dbmap.columns_of(mapper):
                columns.add(column)

        return columns - exclude


    @property
    def group(self):
        return self._group

    @property
    def strict(self):
        """Strict Mode : when true, errors raise exceptions, otherwise skip errors"""
        return self._strict

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
             { 'name': 'column_name1', 'action': 'startswith', 'value': ['a','b'], 'find': 'any' },
             { 'name': 'column_name2', 'action': 'lt', 'value': 37 }
            ]

        * **name: Column Name**

        * action: Filter Operation [Default = `contains`]

        * value: Search Value(s)  - can be either a single value or multiple values.

        * find: Match on Any or All [Default = `any`]  - must match ANY value or ALL values

        **Required** - bold entries are required and have no default
        ** value ** - required for most

        :param data: list of data to filter on
        :raises Error: when strict mode is enabled
        """
        self._filters = []
        for item in data:
            try:
                name = item['name']
                column = self._dbmap[name]
                if column in self.group.exclude or name in self.group.exclude:
                    raise KeyError(name)
                action = item.get('action', 'contains')
                column_filter = column_matcher(column, action)
                value = item.get('value', None)
                if value is None:
                    f = column_filter(column, None)
                else:
                    logic_op = _logical_operations[item.get('find', 'any').lower()]
                    f = reduce(logic_op, (column_filter(column, x) for x in _any_to_many(value)))
                self._filters.append(f)

            except Exception as e:
                if self._strict:
                    raise e
                else:
                    continue

    def apply(self, query):
        if not self._filters:
            return query

        if self.restrictive:
            return query.filter(*self._filters).join(*self.join)

        return query.filter(reduce(_logical_operations['any'], self._filters)).outerjoin(*self.join).distinct()


class StrainerMixin(object):
    """SQLStrainer Mixin - Adds a SQLStrainer instance to Declarative classes."""

    @property
    def strainer(self):
        strainer = getattr(self, '_strainer', None)
        if strainer is None:
            strainer = Strainer(self.__class__)
            setattr(self, '_strainer', strainer)
        return strainer

    def strain(self, query, data):
        self.strainer.strain(data)
        return self.strainer.apply(query)

