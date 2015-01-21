from sqlstrainer.strainer import logical_operations

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'
from match import column_matcher
from marshmallow import Schema
from marshmallow import fields


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


class FilterFieldSchema(Schema):
    __preprocessors__ = [_preprocess_filter]

    def __init__(self, strainer, *args, **kwargs):
        self._strainer = strainer
        kwargs.setdefault('many', True)
        super(FilterFieldSchema, self).__init__(*args, **kwargs)

    # todo: validate column name
    name = fields.String(required=True)
    find = fields.Select(['any', 'all'], default='any')
    # todo: validator for values
    values = fields.List(fields.String, allow_none=True)
    action = fields.String(default='contains')
    not_ = fields.Bool(default=False, attribute='not')

    """ :returns tuple(column, filter)"""
    def make_object(self, data):
        column = self._strainer[data['name']]
        column_filter = column_matcher(column, data['action'])
        values = data['values']
        if values is None:
            f = column_filter(column, None)
        else:
            f = reduce(logical_operations[data['find']], (column_filter(column, x) for x in values))
        return column, f


@FilterFieldSchema.validator
def validate_filter(schema, data):
    strainer = schema._strainer
    col = strainer.get(data['name'])
    if col is None:
        return False
    if column_matcher(col, data.get('action', 'contains')) is None:
        return False
    return True
