"""

"""
__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

from sqlalchemy import inspect
from sqlalchemy.orm.mapper import _mapper_registry, Mapper
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.properties import ColumnProperty, RelationshipProperty
from sqlalchemy.ext.hybrid import hybrid_property
import re



class DBMap():
    _dbmap = None

    def __init__(self, registry=_mapper_registry):
        self._dbmap = self._map_db(registry)

    def get_column(self, name):
        """
        Tries to get a column from the dbmap. tries the whole name, then begins breaking it down to see if it matches a table

        columnname
        """
        for i in range(len(re.findall(r'[._]', name)) + 1, 0, -1):
            columns = self._dbmap.get(name)
            if columns is not None and len(columns) == 1:
                    return columns[0]
            parts = re.split(r'[._]', name)
            name = '{0}.{1}'.format('_'.join(parts[:i-1]), '_'.join(parts[i-1:]))

    @staticmethod
    def get_mapper(obj):
        if not isinstance(obj, Mapper):
            if isinstance(obj, InstrumentedAttribute):
                return getattr(obj.property, 'mapper', None)
            return inspect(obj)
        return obj

    def shortest_path(self, fromObj, toObj):
        fromObj = self.get_mapper(fromObj)
        relations = self._dbmap.get(fromObj)
        if relations is None:
            return None

        toObj = self.get_mapper(toObj)
        return self._shortestPath(relations, toObj)

    @staticmethod
    def first_relation(relations, find):
        # try relationship properties first
        if find in relations:
            return find
        # handle models or mappers
        find = DBMap.get_mapper(find)
        for r, rmap in relations.items():
            if rmap == find:
                return r
        return None

    def relationship_path(self, root, path):
        """Converts a list of models into a list of relationships which can be used in a join

        :param path: list of models or mappers
        :return: list of relationships
        """
        relations = []
        root = self.get_mapper(root)
        children = self._dbmap.get(root)
        if not children:
            return None
        for o in path:
            r = self.first_relation(children, o)
            if not r:
                return None
            relations.append(r)
            root = children.get(r)
            children = self._dbmap.get(root)
            if not children:
                return None
        return relations


    def _shortestPath(self, relations, find, path=None):
        if path is None:
            path = []
        find = DBMap.get_mapper(find)

        # r: relationship, m: mapper
        current = {m: r for r, m in relations.items()}
        found = current.get(find)

        if found:
            path.append(found)
            return path
        sp = []
        for r, m in relations.items():
            nest = self._dbmap.get(m)
            if not nest:
                return None
            p = self._shortestPath(nest, find, path + [r])
            if p is not None:
                sp.append(p)
        if sp:
            return sorted(sp, key=lambda p: len(p))[0]
        return None

    def _map_db(self, registry=_mapper_registry):
        if self._dbmap is not None:
            return self._dbmap
        dbmap = dict()
        for mapper in registry.keys():
            dbmap[mapper] = dict((rprop.class_attribute, rprop.mapper) for rprop in mapper.relationships)
            model = mapper.entity
            for name, o in mapper.all_orm_descriptors.items():
                column = None
                if isinstance(o, InstrumentedAttribute):
                    if isinstance(o.property, ColumnProperty):
                        column = o
                elif isinstance(o, hybrid_property):
                    column = getattr(model, name)
                if column is not None:
                    dbmap.setdefault(name, dict())[mapper] = column
                    dbmap[model.__tablename__ + '.' + name] = dict((mapper, column))

        return dbmap

