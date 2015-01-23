from match import column_matcher
from marshmallow import Schema, UnmarshallingError, ValidationError
from marshmallow import fields
import sqlalchemy as sa


logical_operations = {
    'any': lambda x, y: x | y,
    'all': lambda x, y: x & y,
}


# deprecated
def _preprocess_filter(schema, in_data):
    keys = list(in_data.keys())
    for key in keys:
        parts = key.split('.')
        if len(parts) > 1:
            table = parts[0]
            field = '.'.join(parts[1:])
            in_data.setdefault(table, {})[field] = in_data[key]
            del in_data[key]

    return in_data


class StrainerSchema(Schema):

    def __init__(self, strainer, *args, **kwargs):
        self._strainer = strainer
        kwargs.setdefault('many', True)
# todo: make_object gets called unless strict is true..
#        kwargs['strict'] = True # must be strict!
        super(StrainerSchema, self).__init__(*args, **kwargs)

    name = fields.String(required=True)
    find = fields.Select(['any', 'all'], default='any')
    # todo: validator for values
    values = fields.List(fields.String, allow_none=True)
    action = fields.String(default='contains')
    not_ = fields.Bool(default=False, attribute='not')

    """ :returns tuple(StrainerColumn, BooleanClause)"""
    def make_object(self, data):
        # todo: this gets called even when there are errors, unless strict is set
        try:
            entry = self._strainer.get(data['name'])
            column = entry.column
            column_filter = column_matcher(column, data.get('action', 'contains'))
        except KeyError:
            return data
        values = data.get('values', None)
        if not values:
            f = column_filter(column, None)
        else:
            f = reduce(logical_operations[data.get('find', 'any')], (column_filter(column, x) for x in values))
        if data.get('not_'):
            f = sa.not_(f)
        data['filter'] = f
        return data

from match import deserialize_value_for_column

@StrainerSchema.validator
def validate_filter(schema, data):
    strainer = schema._strainer
    try:
        col = strainer.get(data['name'])
    except KeyError:
        return False
    if col is None:
        return False
    action = data.get('action', 'contains')
    try:
        column_matcher(col.column, action)
    except KeyError:
        return False
    # todo: dont hard code this!
    if action == 'empty' or action == 'notempty':
        return True

    values = data.get('values', None)
    if not values:
        return False
    try:
        # todo: find a better way to deserialize!
        data['values'] = map(lambda x: deserialize_value_for_column(col.column, x), values)
    except (UnmarshallingError, ValidationError):
        return False
    return True


class FilterValueField(fields.Field):
    # todo: create a deserializer for values
    def _deserialize(self, value):
        return {
            'required': set(),
            'optional': set()
        }
