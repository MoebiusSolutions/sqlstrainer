"""Strainer builds a filter path for a mapper/model

At class level call my_model_strainer = Strainer(MyModel)
my_model_strainer.relate('other', [MyModel.other], flags=[Other.foo == 'bar'])

To use:
strainer, errors = my_model_strainer(somedata)
if errors:
    handleErrors(errors)
else:
    query = strainer.strain(session.query(MyModel))

    # To apply the same where statement to another query...
    other_q = session.query(MyModel.id, sum(Other.value)).group_by(MyModel.id).join(MyModel.other)
    query = strainer.strain(other_q)

.. autoclass:: Strainer
    :members:

"""
from sqlalchemy import or_ as sql_or
from sqlalchemy.ext.hybrid import hybrid_property
from six import string_types
from sqlstrainer.mapper import StrainerMap
from sqlstrainer.schema import StrainerSchema

"""strainer map"""
_dbmap = None

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


class _StrainerJoin(object):
    """Local helper class to hold join info
    """

    def __init__(self, name, base, join, flags):
        self._base = base
        self._name = name
        self._mapper = _dbmap.to_mapper(join[-1])
        self._flags = flags
        self._join = join

    def field_name(self, column):
        return self.tablename + '.' + column.split('.')[-1]

    def alias_name(self, column):
        return self._name + '.' + column.split('.')[-1]

    @property
    def tablename(self):
        return self._mapper.entity.__tablename__

    @property
    def name(self):
        return self._name

    @property
    def flags(self):
        return self._flags

    @property
    def join(self):
        return self._join


class StrainerFilter(object):

    def __init__(self, strainer, filters):
        self._strainer = strainer
        self._filters = filters

    def strain(self, query):
        if not self._filters:
            return query
        filters = []
        tables = set()
        basename = self._strainer.tablename
        for f in self._filters:
            filters.append(f['filter'])
            tbl, _ = self._strainer.split_name(f['name'])
            if tbl != basename:
                tables.add(tbl)

        join_type = 'join'
        if self._strainer.restrictive:
            query = query.filter(*filters)
        else:
            query = query.filter(sql_or(*filters))
            join_type = 'outerjoin'
            # todo: validate repeated join relations act as set

        for tbl in tables:
            query = getattr(query, join_type)(*self._strainer.relatives[tbl].join)
            flags = self._strainer.relatives[tbl].flags
            if flags:
                query = query.filter(*flags)

        return query.distinct()


class Strainer(object):
    """Strainer is ...

    >>> strainer = Strainer(User)
    """

    restrictive = True
    VIEW_DISTINCT = 1
    VIEW_NESTED = 2

    def __init__(self, base, strict=False):
        """
        :param base: base entity - all joins originate from here
        :type base: Declarative or Mapper
        :param strict: Strict Mode : errors raise exceptions, default skip errors
        :type strict: bool
        """
        self._relatives = {}
        self._base = base
        self._filters = None
        self._exclude = set()
        self._to_relate = []
        self._initialized = False

    def init(self):
        global _dbmap
        if not _dbmap:
            _dbmap = StrainerMap()
        self._base = _dbmap.to_mapper(self._base)
        self._initialized = True
        for r in self._to_relate:
            self.relate(*r)

    def __call__(self, *args, **kwargs):
        return self.build(args[0])

    def get(self, item):
        # todo: handle alias
        tbl, name = self.split_name(item)

        if tbl != self.tablename:
            path = self._relatives[tbl].join
            tbl = _dbmap.to_mapper(path[-1]).entity.__tablename__

        return _dbmap[tbl + '.' + name]

    @property
    def tablename(self):
        return self._base.entity.__tablename__

    def split_name(self, item):
        parts = item.split('.')
        if len(parts) == 2:
            return parts
        if len(parts) == 1:
            return self.tablename, item

        raise AttributeError(item)

    def build(self, data):
        """builds the filter for the given set of data

        data format::

            [
             { name: 'column_name1', 'action': 'contains', 'value': ['a','b'], 'find': 'any' },
             { name: 'column_name2', 'action': 'lt', 'value': 37 }
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
        if not self._initialized:
            self.init()
        filters, errors = StrainerSchema(self).load(data)
        return StrainerFilter(self, filters), errors

    def relate(self, name, path, flags=None, exclude=None):
        if not self._initialized:
            self._to_relate.append((name, path, flags, exclude))
            return
        # todo: make an alias to name and use the alias for the column getter...
        if isinstance(path, string_types):
            parts = path.split('.')
            if not parts[0] == self.tablename:
                path = self.tablename + '.' + path
            join = _dbmap.join_from_dotted(path)
        else:
            # be forgiving (not sure if good/bad thing)
            try:
                path = list(path)
            except TypeError:
                path = [path]

            mapper = _dbmap.to_mapper(path[0])
            if mapper is not self._base:
                path.insert(0, self._base)
            join = _dbmap.join_path(path)

        self._relatives[name] = _StrainerJoin(name, self._base, join, flags)
        if exclude:
            self.exclude(exclude)

    def exclude(self, *args):
        if args:
            exclude = args
            if len(exclude) == 1 and isinstance(exclude[0], (list, tuple)):
                exclude = exclude[0]
            for field in exclude:
                self._exclude.add(_dbmap[field])
        return self._exclude

    @property
    def relatives(self):
        return self._relatives


class StrainerMixin(object):
    """SQLStrainer Mixin - Adds a SQLStrainer instance to Declarative classes."""

    @property
    def strainer(self):
        strainer = getattr(self, '_strainer', None)
        if strainer is None:
            strainer = Strainer(self.__class__)
            setattr(self, '_strainer', strainer)
        return strainer



