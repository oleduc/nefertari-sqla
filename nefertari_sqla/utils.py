from sqlalchemy.orm.properties import RelationshipProperty
from sqlalchemy.orm import class_mapper, properties

relationship_fields = (
    RelationshipProperty,
)


def is_relationship_field(field, model_cls):
    """ Determine if `field` of the `model_cls` is a relational
    field.
    """
    if not model_cls.has_field(field):
        return False
    mapper = class_mapper(model_cls)
    relationships = {r.key: r for r in mapper.relationships}
    field_obj = relationships.get(field)
    return isinstance(field_obj, relationship_fields)


def get_relationship_cls(field, model_cls):
    """ Return class that is pointed to by relationship field
    `field` from model `model_cls`.

    Make sure field exists and is a relationship
    field manually. Use `is_relationship_field` for this.
     """
    mapper = class_mapper(model_cls)
    relationships = {r.key: r for r in mapper.relationships}
    field_obj = relationships[field]
    return field_obj.mapper.class_


def is_indexable(field):
    return getattr(field, 'es_index', True)


def get_backref_props(cls):
    iter_props = class_mapper(cls).iterate_properties
    backref_props = [p for p in iter_props
                     if isinstance(p, properties.RelationshipProperty)]
    return backref_props


class FieldsQuerySet(list):
    pass
