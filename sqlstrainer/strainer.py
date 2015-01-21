"""Strainer builds a filter path for a mapper/model

.. autoclass:: Strainer
    :members:

"""
from sqlalchemy.ext.hybrid import hybrid_property
from six import string_types
from sqlstrainer.mapper import StrainerMap
from sqlstrainer.match import column_matcher
from sqlstrainer.schema import FilterFieldSchema

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

def _any_to_many(v):
    """list generator for any input: text, numeric or iterable

    wrapped in list produces:
        None -> [], 'test'->['test'], 123 -> [123], (1,2,3) -> [1,2,3]
    """
    if v is None:
        raise StopIteration
    if isinstance(v, string_types):
        yield v
    else:
        try:
            for value in iter(v):
                yield value
        except TypeError:
            yield v


def strainer_property(**info):
    """very simple decorator to markup hybrid_property with info similar to Column(info={})

    instead of:
    @hybrid_property
    def full_name(self):
        return self.first + ' ' + self.last

    use:
    @strainer_property(label='Full Name')
    def full_name(self):
        return self.first + ' ' + self.last

    :param info: kwargs style dict for strainer information
    :return: hybrid_property decorator
    """
    def decorator(func):
        if not info.get('filterable', True):
            ret = property(func)
        else:
            ret = hybrid_property(func)
        ret.fget.info = info
        return ret
    return decorator


class NestablePath(object):
    _name = None
    base = None

    _paths = None

    """list of excluded columns, mappers or relations"""
    _exclude = None

    flags = None

    def __init__(self, base, name=None):
        # TODO: handle flags
        self.base = base
        self._name = name
        self._paths = []
        self._exclude = []

    @property
    def name(self):
        if not self._name:
            self._name = self.base.class_.__name__
        return self._name

    @property
    def paths(self):
        return self._paths

    def get(self, item, default=None):
        if '.' not in item:
            item = self.base.entity.__tablename__ + '.' + item
        entry = self._dbmap.get(item)
        if entry:
            for group in self.all:
                if group.base == entry.mapper:
                    if entry.column in group._exclude: # todo: should check entire exclude chain
                        return default
                    return entry.column

        return default

    def to_path(self, column):
        # todo: handle dotted relationships
        stack = [self]
        path = []
        while stack:
            group = stack.pop()
            path.append(group)
            if group.base == column.mapper:
                return path

            for child in reversed(group._paths):
                stack.append(child)

            if group == path[-1]:
                path.pop()

        return path

    def exclude(self):
        return self._exclude

    @property
    def all(self):
        stack = [self]
        while stack:
            group = stack.pop()
            yield group
            for child in reversed(group._paths):
                stack.append(child)

    def nest(self, base):
        path = NestablePath(base)
        self._paths.append(path)
        return path


logical_operations = {
    'any': lambda x, y: x | y,
    'all': lambda x, y: x & y,
}


class Strainer(object):
    """Strainer is ...

    >>> strainer = Strainer(User)
    """
    _dbmap = None
    _strict = False
    _filters = None
    _columns = None

    restrictive = True
    VIEW_DISTINCT = 1
    VIEW_NESTED = 2

    def __init__(self, base, dbmap=None, strict=False, all_relatives=False):
        """
        :param base: base entity - all joins originate from here
        :type base: Declarative or Mapper
        :param dbmap: DBMap (if empty, a default DBMap will be created)
        :type dbmap: :class:`sqlstrainer.mapper.DBMap`
        :param strict: Strict Mode : errors raise exceptions, default skip errors
        :type strict: bool
        """

        if self.__class__._dbmap is None:
            dbmap = StrainerMap()
        self.__class__._dbmap = dbmap
        mapper = dbmap.to_mapper(base)
        self._group = NestablePath(mapper)
        if all_relatives:
            relatives = dbmap.all_relatives(mapper)
            for relative, paths in relatives.iteritems():
                shortest_path = sorted(paths, key=lambda sp: len(sp))[0]
                self.group.paths.append(shortest_path[0].property)

        self.options = {}
        self._strict = strict
        self._filters = None
        self._columns = None

    def __getitem__(self, item):
        val = self.get(item, None)
        if val is None:
            raise KeyError(item)
        return val

    def get(self, item, default=None):
        return self._group.get(item, default)

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
            if isinstance(obj, string_types):
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

    def strain(self, data):
        """builds the filter for the given set of data

        data format::

            [
             { name: 'column_name1', 'action': 'contains', 'value': ['a','b'], 'find': 'any' },
             'column_name2': {'action': 'lt', 'value': 37 }
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
        filters, errors = FilterFieldSchema(self).load(data)
        if errors:
            raise AttributeError(errors)
        self._filters = filters

    def apply(self, query):
        if not self._filters:
            return query
        filters = []
        join = []
        for column, filter in self._filters:
            if not column.mapper == self.group.base:
                join.append(self.group.to_path(column))

            query = query.filter(*[f for _, f in self._filters])
            if self._columns:
                query = query.join(*self.join)
        else:
            query = query.filter(reduce(logical_operations['any'], self._filters))
            if self._columns:
                query = query.outerjoin(*self.join).distinct()
        return query


class StrainerMixin(object):
    """SQLStrainer Mixin - Adds a SQLStrainer instance to Declarative classes."""

    @property
    def strainer(self):
        strainer = getattr(self, '_strainer', None)
        if strainer is None:
            strainer = Strainer(self.__class__)
            setattr(self, '_strainer', strainer)
        return strainer



