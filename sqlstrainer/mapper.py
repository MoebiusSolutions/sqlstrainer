"""Maps column names and relationship paths

.. autoclass:: DBMap
    :members:

.. autoclass:: ColumnEntry
    :members:
"""
__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

from sqlalchemy import inspect
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.ext.hybrid import hybrid_property
import re

class ColumnEntry(object):
    mapper = None
    column = None
    def __init__(self, mapper, column):
        self.mapper = mapper
        self.column = column

class DBMap():
    """Database Map

    Helper class that holds SQLAlchemy ORM relationship and column information in lookup tables.

    Instantiate with a dictionary of mappers as key and any truthy value.
    If you don't build your own mapper registry, use sqlalchemy's.

    .. code-block:: python

        import sqlalchemy
        dbmap = DBMap(sqlalchemy.orm._mapper_registry)

    """

    _relations = None

    _columns = None
    """a map of all columns and hybrid properties in the ORM

    name: ColumnEntry(mapper, column)

    where name is either a column name unique in the ORM
        or table_name.column_name
    """

    def __init__(self, registry):
        self._relations = dict()
        self._columns = dict()
        unique = dict()

        for mapper, primary in registry.items():
            if not primary:
                continue
            self._relations[mapper] = dict((rprop.class_attribute, rprop.mapper) for rprop in mapper.relationships)
            model = mapper.entity
            for name, o in mapper.all_orm_descriptors.items():
                column = None
                if isinstance(o, InstrumentedAttribute):
                    if isinstance(o.property, ColumnProperty):
                        column = o
                elif isinstance(o, hybrid_property):
                    column = getattr(model, name)
                if column is not None:
                    unique.setdefault(name, []).append((mapper, column))
                    self._columns[model.__tablename__ + '.' + name] = ColumnEntry(mapper, column)
        for name, entries in unique.items():
            if len(entries) == 1:
                self._columns[name] = ColumnEntry(entries[0][0], entries[0][1])

    def __contains__(self, item):
        self.find_column(item) is not None

    def __getitem__(self, item):
        col = self.find_column(item)
        if col is None:
            raise KeyError(item)
        return col

    def find_column(self, name):
        """Search for a column by name

        given: `my_table_my_column`, attempts in order:

        1.    my_table_my_column
        2.    my_table_my.column
        3.    my_table.my_column
        4.    my.table_my_column

        having table `my_table` with column `my_column`

        returns column entry for `my_table.my_column`

        :param name: column_name or table_name.column_name
        :type name: str
        :return: column entry
        :rtype: :class:`ColumnEntry`
        """
        parts = re.split(r'[._]', name)
        for ii in range(len(parts), 0, -1):
            column = self._columns.get(name)
            if column is not None:
                return column
            name = '{0}.{1}'.format('_'.join(parts[:ii-1]), '_'.join(parts[ii-1:]))

    @staticmethod
    def to_mapper(obj):
        """takes a mapper, model or relationship and returns the target mapper"""
        if not isinstance(obj, Mapper):
            if isinstance(obj, InstrumentedAttribute):
                return getattr(obj.property, 'mapper', None)
            return inspect(obj)
        return obj

    def shortest_path(self, fromObj, toObj):
        """Finds the shortest path from one table to another

         Parameters can be any combination of mapper, relationship, or model

         Returns None if there is no path

         :param fromObj: the starting relation
         :param toObj: the target relation
         :return: a list of relationships which can be passed to `Query.join`
         :rtype: list
         """
        fromObj = self.to_mapper(fromObj)
        relations = self._relations.get(fromObj)
        if relations is None:
            return None

        toObj = self.to_mapper(toObj)
        return self._shortestPath(relations, toObj)

    @classmethod
    def first_relation(cls, relations, find):
        """takes a list of relations and finds the first one that matches

        :param relations: a list of :class:`sqlalchemy.orm.properties.RelationshipProperty`
        :param find: the relation to find (Mapper, Model, Relationship)
        :return: first relationship to match
        """

        # try relationship properties first
        if find in relations:
            return find
        # handle models or mappers
        find = cls.to_mapper(find)
        for relationship, mapper in relations.items():
            if mapper == find:
                return relationship
        return None

    def relationship_path(self, root, path):
        """Converts a list of models into a list of relationships which can be used in a join

        :param path: list of models or mappers
        :return: list of relationships
        """
        relations = []
        root = self.to_mapper(root)
        children = self._relations.get(root)
        if not children:
            return None
        for o in path:
            r = self.first_relation(children, o)
            if not r:
                return None
            relations.append(r)
            root = children.get(r)
            children = self._relations.get(root)
            if not children:
                return None
        return relations


    def _shortestPath(self, relations, find, path=None):
        """recursively build all paths to the target, return the shortest"""
        if path is None:
            path = []
        # mapper: relationship
        current = dict((m, r) for r, m in relations.items())
        found = current.get(find)

        if found:
            path.append(found)
            return path
        shortest_path = []
        for r, m in relations.items():
            nest = self._relations.get(m)
            if not nest:
                return None
            p = self._shortestPath(nest, find, path + [r])
            if p is not None:
                shortest_path.append(p)
        if shortest_path:
            return sorted(shortest_path, key=lambda sp: len(sp))[0]
        return None

