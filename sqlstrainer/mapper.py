"""Maps column names and relationship paths

.. autoclass:: DBMap
    :members:

.. autoclass:: ColumnEntry
    :members:
"""
from sqlalchemy import inspect, orm
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.ext.hybrid import hybrid_property
import re

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'


class NoPathAvailable(Exception):
    """Relationship path does not exist"""


class ColumnEntry(object):
    """Simple class to hold mapper and column data"""
    mapper = None
    column = None

    def __init__(self, mapper, column):
        self.mapper = mapper
        self.column = column

    def __repr__(self):
        return '{0}.{1}'.format(self.mapper.entity.__tablename__, self.column.name)


class DBMap():
    """Database Map

    Helper class that holds SQLAlchemy ORM relationship and column information in lookup tables.

    Instantiate with a dictionary of {mapper: is_primary}.
    By default, uses :data:`sqlalchemy.orm._mapper_registry`.
    """

    _relations = None

    _columns = None
    """a map of all columns and hybrid properties in the ORM

    name: ColumnEntry(mapper, column)

    where name is either a column name unique in the ORM
        or table_name.column_name
    """

    def __init__(self, registry=None):
        if registry is None:
            registry = orm._mapper_registry
        self._relations = dict()
        self._columns = dict()
        for mapper, _ in filter(lambda _, is_primary: is_primary, registry.iteritems()):
            self._relations[mapper] = dict((rprop.class_attribute, rprop.mapper) for rprop in mapper.relationships)
            model = mapper.entity
# Handle Viewables
            for name, o in mapper.all_orm_descriptors.items():
                column = None
                if isinstance(o, InstrumentedAttribute):
                    if isinstance(o.property, ColumnProperty):
                        column = o
                elif isinstance(o, hybrid_property):
                    column = getattr(model, name)
                if column is not None:
                    self._columns.setdefault(name, []).append(ColumnEntry(mapper, column))
                    self._columns[model.__tablename__ + '.' + name] = [ColumnEntry(mapper, column)]

    def __contains__(self, item):
        self.get(item) is not None

    def __getitem__(self, item):
        col = self.get(item)
        if col is None:
            raise KeyError(item)
        return col

    def get(self, item, default=None):
        """get a unique ColumnEntry using :meth:`.find_columns`"""
        col = self.find_columns(item)
        if col is not None and len(col) == 1:
            return col[0]

    def get_mapper(self, tablename, default=None):
        """get a mapper based on tablename"""
        for mapper in self._relations:
            for table in mapper.tables:
                if table.name == tablename:
                    return mapper


    def find_columns(self, name):
        """Search for a column by name

        where name is `my_table_my_column` attempt:

        1.    my_table_my_column
        2.    my_table_my.column
        3.    my_table.my_column
        4.    my.table_my_column

        having table `my_table` with column `my_column`

        returns a list of entries for `my_table.my_column`

        Use :meth:`get` to retrieve a unique column entry

        :param name: column name
        :type name: str
        :return: a list of column entries matching name
        :rtype: list of :class:`ColumnEntry`
        """
        parts = re.split(r'[._]', name)
        for ii in range(len(parts), 0, -1):
            column = self._columns.get(name)
            if column is not None:
                return column
            name = '{0}.{1}'.format('_'.join(parts[:ii-1]), '_'.join(parts[ii-1:]))

    def columns_of(self, obj):
        mapper = self.to_mapper(obj)
        return set((name, entries[0].column) for name, entries in self._columns.iteritems()
                   if '.' in name and entries[0].mapper == mapper)

    @staticmethod
    def to_mapper(obj):
        """takes a mapper, model or relationship and returns the target mapper"""
        obj = inspect(obj)
        if obj.is_attribute:
            obj = getattr(obj.property, 'mapper', obj)
        if not obj.is_mapper:
            raise TypeError('object must be a mapper, model or relationship')
        return obj

    def all_relatives(self, obj):
        """Retrieves all descendants of a mapper or model

        >>> dbmap.all_relatives(Base)
        {
            Mapper: [ [Rel1, Rel2], [Rel3, Rel4, Rel5] ],
            ...
        }
        # Base->Rel1->Rel2->Mapper, Base->Rel3->Rel4->Rel5->Mapper

        :param obj: the parent model
        :type obj: Declarative | Mapper
        :return: list of descendants
        """

        relatives = {}
        stack = [(self.to_mapper(obj), [])]
        while stack:
            parent, root = stack.pop()
            children = self._relations.get(parent)
            for relationship, mapper in children.iteritems():
                path = root + [relationship]
                if mapper not in relatives:
                    stack.append((mapper, path))
                relatives.setdefault(mapper, []).append(path)
        return relatives

    def shortest_path(self, from_obj, to_obj):
        """Finds the shortest path from one table to another

         Parameters can be any combination of mapper, relationship, or model

         Returns None if there is no path

         :param from_obj: the starting relation
         :param to_obj: the target relation
         :return: a list of relationships which can be passed to `Query.join`
         :rtype: list
         """
        relatives = self.all_relatives(from_obj)
        routes = relatives.get(self.to_mapper(to_obj))
        if routes:
            return sorted(routes, key=lambda sp: len(sp))[0]

    @classmethod
    def first_relation(cls, relations, find):
        """takes a list of relations and finds the first one that matches

        :param relations: a list of relationships
        :param find: the relation to find (Mapper, Model, Relationship)
        :return: first relationship to match
        """

        # try relationship properties first
        if find in relations:
            return find
        # handle models or mappers
        find = cls.to_mapper(find)
        for relationship, mapper in relations.iteritems():
            if mapper == find:
                return relationship
        return None

    def join_path(self, path):
        """Converts a list of models into a list of relationships which can be used in a join

        Returns None if any element of the path is not part of a join path

        :param path: list of models, mappers or relationships
        :return: list of relationships
        :rtype: list
        :raises: NoPathAvailable: an element in the path missing
        """
        relations = []
        root = self.to_mapper(path[0])
        path = path[1:]
        children = self._relations.get(root)
        if not children:
            raise NoPathAvailable
        for o in path:
            r = self.first_relation(children, o)
            if not r:
                raise NoPathAvailable
            relations.append(r)
            root = children.get(r)
            children = self._relations.get(root)
            if not children:
                raise NoPathAvailable
        return relations

    def relations_of(self, mapper):
        """Lists the relations that are available after join path

        :param path: list of models, mappers or relationships
        :return: list of relationships
        :rtype: dict(RelationshipProperty, Mapper)
        """
        return self._relations[mapper]


