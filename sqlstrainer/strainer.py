"""

"""
from sqlstrainer.mapper import DBMap

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'


class Strainer(object):
    """

    """

    def __init__(self, base, dbmap=None):
        if dbmap is None:
            from sqlalchemy import orm
            dbmap = DBMap(orm._mapper_registry)
        self.dbmap = dbmap
        self.mapper = dbmap.to_mapper(base)
        self.routes = {}
        self.hidden = set()
        self.options = {}

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

    def add_filter(self, obj, filter):
        mapper = self.dbmap.to_mapper(obj)
        self.options[mapper] = filter

    def add_value(self, data):
        raise NotImplemented

    def strain(self, data):
        raise NotImplemented


column_handlers = {}


class ColumnStrainer(object):
    """

    """

    def __init__(self, column):
        self.handler = column_handlers.get(column.type, column_handlers['DEFAULT'])


class TextStrainer(ColumnStrainer):
    match = '%{}%'
    callables = {}

    def strain(self, key, data):

#nopes
        if self.data[key] is None:
            self.column.ilike(self.match.format(data))

#        self.column[key] = callables[self.column[key]].type](data)
