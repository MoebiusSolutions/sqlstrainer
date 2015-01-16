__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'
import match
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


class BaseSchema(Schema):
    __preprocessors__ = [_preprocess_filter]

    # def __init__(self, *args, **kwargs):
    #     kwargs.setdefault('many', True)
    #     super(BaseSchema, self).__init__(*args, **kwargs)

    def dump(self, obj, many=None, update_fields=False, **kwargs):
        return super(BaseSchema, self).dump(obj, many, update_fields, **kwargs)


class FilterFieldSchema(BaseSchema):
    find = fields.Select(['any', 'all'], default='any')
    values = fields.List(fields.Str)
    action = fields.Select(match._default.keys(), default='contains')
    not_ = fields.Bool(default=False, attribute='not')


class NumberFilterFieldSchema(FilterFieldSchema):
    values = fields.List(fields.Int)
    action = fields.Select(match._numeric.keys(), default='is')


class StringFilterFieldSchema(FilterFieldSchema):
    values = fields.List(fields.Str)
    action = fields.Select(match._string.keys(), default='contains')


class DateTimeFilterFieldSchema(FilterFieldSchema):
    values = fields.List(fields.DateTime)
    action = fields.Select(match._datetime.keys(), default='ibound')


StringFilterSchema = lambda *args, **kwargs: fields.Nested(StringFilterFieldSchema, *args, **kwargs)
DateTimeFilterSchema = lambda *args, **kwargs: fields.Nested(DateTimeFilterFieldSchema, *args, **kwargs)
NumberFilterSchema = lambda *args, **kwargs: fields.Nested(NumberFilterFieldSchema, *args, **kwargs)
