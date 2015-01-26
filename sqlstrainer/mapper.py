"""Maps column names and relationship paths

.. autoclass:: DBMap
    :members:

.. autoclass:: ColumnEntry
    :members:
"""
from six import iteritems, string_types
from sqlalchemy import inspect
from sqlalchemy.orm import _mapper_registry
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.ext.hybrid import hybrid_property

class NoPathAvailable(Exception):
    """Relationship path does not exist"""


class _StrainerColumn(object):
    """Simple class to hold mapper and column data"""
    # COLUMN = 'column'
    # HYBRID = 'hybrid'
    # PROPERTY = 'property'
    # ext = COLUMN

    def __init__(self, mapper, name, column, label=None, viewable=True, filterable=True, **other_info):
        self.mapper = mapper
        self.name = name
        self._column = column
        self.label = label if label is not None else name.replace('_', ' ').title()
        self.viewable = viewable
        if not (isinstance(column, InstrumentedAttribute) or isinstance(column, hybrid_property)):
            # cannot filter on instance properties
            filterable = False
        self.filterable = filterable

    @property
    def column(self):
        if isinstance(self._column, InstrumentedAttribute):
            return self._column
        if isinstance(self._column, hybrid_property):
            return getattr(self.mapper.entity, self.name)
        # should not be called on property

    def __repr__(self):
        return '<StrainerColumn {0}.{1}>'.format(self.mapper.entity.__tablename__, self.name)


class StrainerMap():
    """Database Map

    Helper class that holds SQLAlchemy ORM relationship and column information in lookup tables.

    Instantiate with a dictionary of {mapper: is_primary}.
    By default, uses :data:`sqlalchemy.orm._mapper_registry`.
    """

    def __init__(self, registry=None):
        if registry is None:
            registry = _mapper_registry
        self._columns = {}
        self._relations = {}
        # filters non-primary entries
        for mapper, _ in filter(lambda x: x[1], iteritems(registry)):
            for rprop in mapper.relationships:
                rels = self._relations.setdefault(mapper, {})
                rels[rprop.class_attribute] = rprop.mapper# .self_and_descendants

            cls = mapper.class_
            tbl = cls.__tablename__
            model = self._columns[tbl] = {}
            exclude = set()
            for supercls in mapper.class_.__mro__:
                for key in set(supercls.__dict__).difference(exclude):
                    exclude.add(key)
                    o = supercls.__dict__[key]
                    exclude.add(key)
                    if isinstance(o, InstrumentedAttribute) and isinstance(o.property, ColumnProperty):
                        info = o.info
                    elif hasattr(o, 'fget') and hasattr(o.fget, 'info'):
                        info = o.fget.info
                    else:
                        continue
                    model[key] = _StrainerColumn(mapper, key, o, **info)

    def __contains__(self, item):
        self.get(item) is not None

    def __getitem__(self, item):
        obj = self.get(item)
        if obj is None:
            raise KeyError(item)
        return obj

    def viewable(self, mapper):
        tbl = mapper.entity.__tablename__
        for name, col in iteritems(self._columns[tbl]):
            # TODO: exclude PASSWORD
            yield ('{0}.{1}'.format(tbl, name), col.label)

    def get(self, item, default=None):
        """Fetch a Mapper, StrainerColumn or Relationship based on string key"""
        # TODO: handle receiving columns and hybrids and returning StrainerColumn for exclude
        parts = item.split('.')
        found = list(filter(lambda m: m.entity.__tablename__ == parts[0], self._relations.keys()))
        if not found:
            return default
        mapper = found[0]
        if len(parts) == 1:
            return mapper

        model = self._columns.get(parts[0], None)
        if model:
            col = model.get(parts[1], None)
            if col:
                return col

        found = list(filter(lambda r: r.key == parts[1], self._relations[mapper].keys()))
        if found:
            return found[0]
        return default

    def columns_of(self, obj):
        mapper = self.to_mapper(obj)
        columns = self._columns[mapper.entity.__tablename__]
        for name, sc in iteritems(columns):
            yield name, sc

    def get_mapper(self, tablename, default=None):
        """get a mapper based on tablename"""
        for mapper in self._relations:
            if mapper.entity.__tablename__ == tablename:
                return mapper
        return default

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
            for relationship, mapper in iteritems(children):
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
        for relationship, mapper in iteritems(relations):
            if mapper == find:
                return relationship
        return None

    @classmethod
    def polymorphic_relation(cls, base, find):
        """takes a list of relations and finds the first one that matches

        :param base: a base mapper
        :param find: the child to find (String, Mapper, Model, Relationship)
        :return: first mapper to match
        """
        if isinstance(find, string_types):
            return base.polymorphic_map.get(find, None)

        find = cls.to_mapper(find)
        for child in base.polymorphic_iterator():
            if find == child:
                return child

    def join_path(self, path):
        """Converts a list of models into a list of relationships which can be used in a join

        :param path: list of models, mappers or relationships
        :return: list of relationships
        :rtype: list
        :raises: NoPathAvailable: an element in the path missing
        """

        relations = []
        root = self.to_mapper(path.pop(0))
        if not path:
            raise NoPathAvailable
        children = self._relations.get(root)
        if not children:
            raise NoPathAvailable
        for o in path:
            r = self.first_relation(children, o)
            if r:
                relations.append(r)
                root = children.get(r)
            else:
                root = self.polymorphic_relation(root, o)
                if not root:
                    raise NoPathAvailable
                relations.append(root) # mapper, not relationship.. will work in join?
            children = self._relations.get(root)
            if not children:
                raise NoPathAvailable
        return relations

    def relations_of(self, mapper):
        """Lists the relations that are available for a mapper

        :param path: list of models, mappers or relationships
        :return: list of relationships
        :rtype: dict(RelationshipProperty, Mapper)
        """
        return self._relations[mapper]

    def join_from_dotted(self, dottedPath):
        path = dottedPath.split('.')
        if len(path) < 2:
            raise AttributeError(dottedPath)

        mapper = self.get_mapper(path.pop(0))
        join = []

        for name in path:
            relations = list(filter(lambda r: r.key == name, self._relations[mapper]))
            if len(relations) == 1:
                join.append(relations[0])
                mapper = self.to_mapper(relations[0])
            else:
                child = self.polymorphic_relation(mapper, name)
                if not child:
                    raise NoPathAvailable(dottedPath)
                join.append(child)
                mapper = child
        return join
