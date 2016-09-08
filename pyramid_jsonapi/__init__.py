'''Tools for constructing a JSON-API from sqlalchemy models in Pyramid.'''
import logging

import sqlalchemy
from pyramid.httpexceptions import (
    HTTPForbidden,
    HTTPError
)
import types
import importlib
from collections import deque
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.ext.declarative.api import DeclarativeMeta
import inflection
from pyramid_jsonapi.collection_view_base import CollectionViewBase

__version__ = 0.3

ONETOMANY = sqlalchemy.orm.interfaces.ONETOMANY
MANYTOMANY = sqlalchemy.orm.interfaces.MANYTOMANY
MANYTOONE = sqlalchemy.orm.interfaces.MANYTOONE

view_classes = {}

log = logging.getLogger(__name__)


def error(e, request):
    request.response.content_type = 'application/vnd.api+json'
    request.response.status_code = e.code
    return {
        'errors': [
            {
                'code': str(e.code),
                'detail': e.detail,
                'title': e.title,
            }
        ]
    }


def create_jsonapi(
        config, models, get_dbsession=None,
        engine=None, test_data=None
):
    '''Auto-create jsonapi from module or iterable of sqlAlchemy models.

    Arguments:
        config: ``pyramid.config.Configurator`` object from current app.
        models: an iterable (or module) of model classes derived
            from DeclarativeMeta.
        get_dbsession: a callable shich returns a
            sqlalchemy.orm.session.Session or equivalent.

    Keyword Args:
        engine: a sqlalchemy.engine.Engine instance. Only required if using the
            debug view.
        test_data: a module with an ``add_to_db()`` method which will populate
            the database.
    '''

    config.add_notfound_view(error, renderer='json')
    config.add_forbidden_view(error, renderer='json')
    config.add_view(error, context=HTTPError, renderer='json')

    # Build a list of declarative models to add as collections.
    if isinstance(models, types.ModuleType):
        model_list = []
        for attr in models.__dict__.values():
            if isinstance(attr, DeclarativeMeta):
                try:
                    keycols = sqlalchemy.inspect(attr).primary_key
                except sqlalchemy.exc.NoInspectionAvailable:
                    # Trying to inspect the declarative_base() raises this
                    # exception. We don't want to add it to the API.
                    continue
                model_list.append(attr)
    else:
        model_list = list(models)

    settings = config.registry.settings

    # Add the debug endpoints if required.
    if settings.get('pyramid_jsonapi.debug.debug_endpoints', 'false') == 'true':
        if engine is None:
            DebugView.engine = model_list[0].metadata.bind
        else:
            DebugView.engine = engine
        DebugView.metadata = model_list[0].metadata
        if test_data is None:
            test_data = importlib.import_module(
                settings.get('pyramid_jsonapi.debug.test_data_module', 'test_data')
            )
        DebugView.test_data = test_data
        config.add_route('debug', '/debug/{action}')
        config.add_view(
            DebugView,
            attr='drop',
            route_name='debug',
            match_param='action=drop',
            renderer='json'
        )
        config.add_view(
            DebugView,
            attr='populate',
            route_name='debug',
            match_param='action=populate',
            renderer='json'
        )
        config.add_view(
            DebugView,
            attr='reset',
            route_name='debug',
            match_param='action=reset',
            renderer='json'
        )

    # Loop through the models list. Create resource endpoints for these and
    # any relationships found.
    for model_class in model_list:
        create_resource(config, model_class, get_dbsession=get_dbsession)

create_jsonapi_using_magic_and_pixie_dust = create_jsonapi


def create_resource(
        config, model, get_dbsession,
        collection_name=None, expose_fields=None,
):
    '''Produce a set of resource endpoints.

    Arguments:
        config: ``pyramid.config.Configurator`` object from current app.
        model: a model class derived from DeclarativeMeta.
        get_dbsession: a callable shich returns a
            sqlalchemy.orm.session.Session or equivalent.

    Keyword Args:
        collection_name: string name of collection. Passed through to
            ``collection_view_factory()``
        expose_fields: set of field names to be exposed. Passed through to
            ``collection_view_factory()``
    '''

    # Find the primary key column from the model and add it as _jsonapi_id.
    try:
        keycols = sqlalchemy.inspect(model).primary_key
    except sqlalchemy.exc.NoInspectionAvailable:
        # Trying to inspect the declarative_base() raises this exception. We
        # don't want to add it to the API.
        return
    # Only deal with one primary key column.
    if len(keycols) > 1:
        raise Exception(
            'Model {} has more than one primary key.'.format(
                model.__name__
            )
        )
    model._jsonapi_id = getattr(model, keycols[0].name)

    if collection_name is None:
        collection_name = sqlalchemy.inspect(model).tables[0].name

    collection_name = inflection.pluralize(collection_name)
    log.debug(collection_name)

    # Create a view class for use in the various add_view() calls below.
    view = collection_view_factory(
        config, model, get_dbsession, collection_name,
        expose_fields=expose_fields
    )
    view_classes['collection_name'] = view
    view_classes[model] = view

    settings = config.registry.settings
    view.default_limit = \
        int(settings.get('pyramid_jsonapi.paging.default_limit', 10))
    view.max_limit = \
        int(settings.get('pyramid_jsonapi.paging.max_limit', 100))

    # individual item
    config.add_route(view.item_route_name, view.item_route_pattern)
    # GET
    config.add_view(
        view, attr='get', request_method='GET',
        route_name=view.item_route_name, renderer='json'
    )
    # DELETE
    config.add_view(
        view, attr='delete', request_method='DELETE',
        route_name=view.item_route_name, renderer='json'
    )
    # PATCH
    config.add_view(
        view, attr='patch', request_method='PATCH',
        route_name=view.item_route_name, renderer='json'
    )

    # collection
    config.add_route(view.collection_route_name, view.collection_route_pattern)
    # GET
    config.add_view(
        view, attr='collection_get', request_method='GET',
        route_name=view.collection_route_name, renderer='json'
    )
    # POST
    config.add_view(
        view, attr='collection_post', request_method='POST',
        route_name=view.collection_route_name, renderer='json'
    )

    # related
    config.add_route(view.related_route_name, view.related_route_pattern)
    # GET
    config.add_view(
        view, attr='related_get', request_method='GET',
        route_name=view.related_route_name, renderer='json'
    )

    # relationships
    config.add_route(
        view.relationships_route_name,
        view.relationships_route_pattern
    )
    # GET
    config.add_view(
        view, attr='relationships_get', request_method='GET',
        route_name=view.relationships_route_name, renderer='json'
    )
    # POST
    config.add_view(
        view, attr='relationships_post', request_method='POST',
        route_name=view.relationships_route_name, renderer='json'
    )
    # PATCH
    config.add_view(
        view, attr='relationships_patch', request_method='PATCH',
        route_name=view.relationships_route_name, renderer='json'
    )
    # DELETE
    config.add_view(
        view, attr='relationships_delete', request_method='DELETE',
        route_name=view.relationships_route_name, renderer='json'
    )


def collection_view_factory(
        config,
        model,
        get_dbsession=None,
        collection_name=None,
        expose_fields=None
):
    '''Build a class to handle requests for model.

    Arguments:
        config: ``pyramid.config.Configurator`` object from current app.
        model: a model class derived from DeclarativeMeta.
        get_dbsession: a callable shich returns a
            sqlalchemy.orm.session.Session or equivalent.

    Keyword Args:
        collection_name: string name of collection.
        expose_fields: set of field names to expose.
    '''
    if collection_name is None:
        collection_name = model.__tablename__

    log.debug(collection_name)

    collection_view = type(
        'CollectionView<{}>'.format(collection_name),
        (CollectionViewBase, ),
        {}
    )

    def add_prefix(key, default, sep, name):
        ''''''
        prefix = config.registry.settings.get(key, default)
        if prefix:
            return sep.join((prefix, name))
        else:
            return name

    def add_route_name_prefix(name):
        return add_prefix(
            'pyramid_jsonapi.route_name_prefix', 'pyramid_jsonapi',
            ':', name
        )

    def add_route_pattern_prefix(name):
        return add_prefix(
            'pyramid_jsonapi.route_pattern_prefix', '',
            '/', name
        )

    collection_view.model = model
    collection_view.key_column = sqlalchemy.inspect(model).primary_key[0]
    collection_view.collection_name = collection_name
    collection_view.get_dbsession = get_dbsession

    collection_view.collection_route_name = add_route_name_prefix(
        collection_name
    )
    collection_view.collection_route_pattern = add_route_pattern_prefix(
        collection_name
    )

    collection_view.item_route_name = \
        collection_view.collection_route_name + ':item'
    collection_view.item_route_pattern = \
        collection_view.collection_route_pattern + '/{id}'

    collection_view.related_route_name = \
        collection_view.collection_route_name + ':related'
    collection_view.related_route_pattern = \
        collection_view.collection_route_pattern + '/{id}/{relationship}'

    collection_view.relationships_route_name = \
        collection_view.collection_route_name + ':relationships'
    collection_view.relationships_route_pattern = \
        collection_view.collection_route_pattern + \
        '/{id}/relationships/{relationship}'

    collection_view.exposed_fields = expose_fields
    atts = {}
    fields = {}
    for key, col in sqlalchemy.inspect(model).mapper.columns.items():
        if key == collection_view.key_column.name:
            continue
        if len(col.foreign_keys) > 0:
            continue
        if expose_fields is None or key in expose_fields:
            atts[key] = col
            fields[key] = col
    collection_view.attributes = atts
    rels = {}
    for key, rel in sqlalchemy.inspect(model).mapper.relationships.items():
        if expose_fields is None or key in expose_fields:
            rels[key] = rel
    collection_view.relationships = rels
    fields.update(rels)
    collection_view.fields = fields

    # All callbacks have the current view as the first argument. The comments
    # below detail subsequent args.
    collection_view.callbacks = {
        'after_serialise_identifier': deque(),  # args: identifier(dict)
        'after_serialise_object': deque(),      # args: object(dict)
        'after_get': deque(),                   # args: document(dict)
        'before_patch': deque(),                # args: partial_object(dict)
        'before_delete': deque(),               # args: item(sqlalchemy)
        'after_collection_get': deque(),        # args: document(dict)
        'before_collection_post': deque(),      # args: object(dict)
        'after_related_get': deque(),           # args: document(dict)
        'after_relationships_get': deque(),     # args: document(dict)
        'before_relationships_post': deque(),   # args: object(dict)
        'before_relationships_patch': deque(),  # args: partial_object(dict)
        'before_relationships_delete':
            deque(),                            # args: parent_item(sqlalchemy)
    }

    return collection_view


def acso_after_serialise_object(view, obj):
    '''Standard callback altering object to take account of permissions.

    Args:
        obj (dict): the object immediately after serialisation.

    Returns:
        dict: the object, possibly with some fields removed, or meta
        information indicating permission was denied to the whole object.
    '''
    if view.allowed_object(obj):
        # Remove any forbidden fields that have been added by other
        # callbacks. Those from the model won't have been added in the first
        # place.

        # Keep track so we can tell the caller which ones were forbidden.
        forbidden = set()
        if 'attributes' in obj:
            atts = {}
            for name, val in obj['attributes'].items():
                if name in view.allowed_fields:
                    atts[name] = val
                else:
                    forbidden.add(name)
            obj['attributes'] = atts
        if 'relationships' in obj:
            rels = {}
            for name, val in obj['relationships'].items():
                if name in view.allowed_fields:
                    rels[name] = val
                else:
                    forbidden.add(name)
            obj['relationships'] = rels
        # Now add all the forbidden fields from the model to the forbidden
        # list. They don't need to be removed from the serialised object
        # because they should not have been added in the first place.
        for field in view.requested_field_names:
            if field not in view.allowed_fields:
                forbidden.add(field)
        if 'meta' not in obj:
            obj['meta'] = {}
        obj['meta']['forbidden_fields'] = list(forbidden)
        return obj
    else:
        return {
            'type': obj['type'],
            'id': obj['id'],
            'meta': {
                'errors': [
                    {
                        'code': 403,
                        'title': 'Forbidden',
                        'detail': 'No permission to view {}/{}.'.format(
                            obj['type'], obj['id']
                        )
                    }
                ]
            }
        }


def acso_after_get(view, ret):
    '''Standard callback throwing 403 (Forbidden) based on information in meta.

    Args:
        ret (dict): dict which would have been returned from get().

    Returns:
        dict: the same object if an error has not been raised.

    Raises:
        HTTPForbidden
    '''
    obj = ret['data']
    errors = []
    try:
        errors = obj['meta']['errors']
    except KeyError:
        return ret
    for error in errors:
        if error['code'] == 403:
            raise HTTPForbidden(error['detail'])
    return ret


callback_sets = {
    'access_control_serialised_objects': {
        'after_serialise_object': acso_after_serialise_object,
        'after_get': acso_after_get
    }
}


def append_callback_set_to_all_views(set_name):
    '''Append a named set of callbacks to all view classes.

    Args:
        set_name (str): key in ``callback_sets``.
    '''
    for view_class in view_classes.values():
        view_class.append_callback_set(set_name)


class DebugView:
    '''Pyramid view class defining a debug API.

    These are available as ``/debug/{action}`` if
    ``pyramid_jsonapi.debug.debug_endpoints == 'true'``.

    Attributes:
        engine: sqlalchemy engine with connection to the db.
        metadata: sqlalchemy model metadata
        test_data: module with an ``add_to_db()`` method which will populate
            the database
    '''
    def __init__(self, request):
        self.request = request

    def drop(self):
        '''Drop all tables from the database!!!
        '''
        self.metadata.drop_all(self.engine)
        return 'dropped'

    def populate(self):
        '''Create tables and populate with test data.
        '''
        # Create or update tables and schema. Safe if tables already exist.
        self.metadata.create_all(self.engine)
        # Add test data. Safe if test data already exists.
        self.test_data.add_to_db()
        return 'populated'

    def reset(self):
        '''The same as 'drop' and then 'populate'.
        '''
        self.drop()
        self.populate()
        return "reset"
