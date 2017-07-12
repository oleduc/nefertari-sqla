import logging

from sqlalchemy import event
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import object_session, class_mapper, attributes
from pyramid_sqlalchemy import Session
from nefertari.elasticsearch import ES


log = logging.getLogger(__name__)


def index_object(obj, with_refs=True, **kwargs):
    es = ES(obj.__class__.__name__)
    es.index(obj, **kwargs)
    if with_refs:
        es.index_relations(obj, **kwargs)


def on_after_insert(mapper, connection, target):
    # Reload `target` to get access to back references and processed
    # fields values
    model_cls = target.__class__
    pk_field = target.pk_field()
    reloaded = model_cls.get_item(
        **{pk_field: getattr(target, pk_field)})
    index_object(reloaded)


def on_after_update(mapper, connection, target):
    from .documents import BaseDocument

    # Reindex old one-to-one related object
    committed_state = attributes.instance_state(target).committed_state
    columns = set()
    for field, value in committed_state.items():
        if isinstance(value, BaseDocument):
            obj_session = object_session(value)
            # Make sure object is not updated yet
            if not obj_session.is_modified(value):
                obj_session.expire(value)
            index_object(value, with_refs=False)
        else:
            id_pos = field.rfind('_id')
            if id_pos >= 0:
                rel_name = field[:id_pos]
                rel = mapper.relationships.get(rel_name, False)
                if rel and any(c.name == field for c in rel.local_columns):
                    columns.add(rel_name)

    # Reload `target` to get access to processed fields values
    columns = columns.union([c.name for c in class_mapper(target.__class__).columns])
    object_session(target).expire(target, attribute_names=columns)
    index_object(target, with_refs=False, nested_only=True)

    # Reindex the item's parents. This must be done after the child has been processes
    for parent, children_field in target.get_parent_documents(nested_only=True):
        columns = [c.name for c in class_mapper(parent.__class__).columns]
        object_session(parent).expire(parent, attribute_names=columns)
        ES(parent.__class__.__name__).index_nested_document(parent, children_field, target)


def on_after_delete(mapper, connection, target):
    from nefertari.elasticsearch import ES
    model_cls = target.__class__
    es = ES(model_cls.__name__)
    obj_id = getattr(target, model_cls.pk_field())
    es.delete(obj_id)
    target.expire_parents()
    es.index_relations(target)


def on_bulk_update(update_context):
    model_cls = update_context.mapper.entity
    if not getattr(model_cls, '_index_enabled', False):
        return

    objects = update_context.query.all()
    if not objects:
        return

    from nefertari.elasticsearch import ES
    es = ES(source=model_cls.__name__)
    es.index(objects)

    # Reindex relationships
    es.bulk_index_relations(objects, nested_only=True)


def on_bulk_delete(model_cls, objects):
    if not getattr(model_cls, '_index_enabled', False):
        return

    pk_field = model_cls.pk_field()
    ids = [getattr(obj, pk_field) for obj in objects]

    from nefertari.elasticsearch import ES
    es = ES(source=model_cls.__name__)
    es.delete(ids)
    model_cls.bulk_expire_parents(objects)
    # Reindex relationships
    es.bulk_index_relations(objects)


def setup_es_signals_for(source_cls):
    event.listen(source_cls, 'after_insert', on_after_insert)
    event.listen(source_cls, 'after_update', on_after_update)
    event.listen(source_cls, 'after_delete', on_after_delete)
    log.info('setup_sqla_es_signals_for: %r' % source_cls)


event.listen(Session, 'after_bulk_update', on_bulk_update)


class ESMetaclass(DeclarativeMeta):
    # This allows us to use duck typing to test type without importing nefertari_sqla into nefertari
    is_ESMetaclass = True

    def __init__(self, name, bases, attrs):
        self._index_enabled = True
        setup_es_signals_for(self)
        super(ESMetaclass, self).__init__(name, bases, attrs)
