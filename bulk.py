'''
Django bulk operations on simple models.
Does not attempt to cover all corner cases and related models.

Originally from http://people.iola.dk/olau/python/bulkops.py

'''
from functools import wraps
from itertools import repeat
from django.db import models, connections, transaction


def _model_keys(model, field_names=None):
    """Takes a model class and returns a list of fields that should be
       used in a WHERE clause to an UPDATE.

    :param model: A model class.
    :param field_names: An iterable of field names that should be returned if
        they match field in the model. If empty, no fields are returned.
        If None the model's primary key is returned.
    :returns: A list of fields that should be used in a WHERE
    """
    # If no fields are specified, use the primary key by default
    if field_names is None:
        return [model._meta.pk]
    field_names = set(field_names)
    fields = []
    for f in model._meta.fields:
        if field_names and f.name not in field_names:
            continue
        fields.append(f)
    return fields


def _model_fields(model, field_names=None):
    """Takes a model class and returns a list of fields that should be
        inserted/updated.

    :param model: A model class.
    :param field_names: An iterable of field names that should be returned if
        they match field in the model. If None or empty all fields of the
        models that should be update or inserted are returned.
    :returns: A list of fields that should be updated/inserted.
    """
    fields = []
    field_names = set(field_names or [])
    for f in model._meta.fields:
        is_autofield = isinstance(f, models.AutoField)
        should_skip_field = field_names and f.name not in field_names
        if is_autofield or should_skip_field:
            continue
        fields.append(f)
    return fields


def _split_model_fields(model, keys=None, update_fields=None,
                        exclude_fields=None):
    """Separate model fields in key and value fields.

    :param model: Django model class.
    :param keys: An iterable of field names to update on. If none,
        the model's primary key is returned.
    :param update_fields: An iterable of field names up be updated. If none
        or empty, all fields of the model are updated.
    :param exclude_fields: An iterable of field names to be excluded from
        the set of model fields to be updated.
    :raises ValueError: if keys is not None and empty
    """

    key_fields = _model_keys(model, keys)
    if not key_fields:
        raise ValueError("Empty key fields")

    # don't update the key fields
    excluded_field_names = set(field.name for field in key_fields)
    excluded_field_names.update(exclude_fields or [])

    value_fields = [
        field for field in _model_fields(model, update_fields)
        if field.name not in excluded_field_names
    ]
    return (key_fields, value_fields)


def _prep_values(fields, obj, con, add):
    if hasattr(obj, 'presave') and callable(obj.presave):
        obj.presave()

    values = []
    for f in fields:
        field_type = f.get_internal_type()
        if field_type in ('DateTimeField', 'DateField', 'UUIDField'):
            v = f.pre_save(obj, add)
            # FIXME: This is necessary for when a DateTimeField is present in
            # a `keys` parameter of `insert_or_update_many`. Newer versions of
            # Django make the fields tz aware. The problem here is that
            # comparing two `datetime` objects with the *same* value but one
            # being tz aware the other not, actually fails.
            # It looks like postgresql stores things in UTC by default, so
            # the code below is dropping the tz info at the Django side.
            # This is not an elegant solution and also relies on a big
            # assumption which may not be true (PostgreSQL always in UTC).
            if field_type == 'DateTimeField':
                if v is not None:
                    try:
                        v = v.replace(tzinfo=None)
                    except TypeError:
                        # DateTimeField with no tzinfo
                        pass
            values.append(v)
        else:
            values.append(f.get_db_prep_save(f.pre_save(obj, add),
                                             connection=con))
    return tuple(values)


def _build_rows(fields, parameters):
    fields_name = [f.name for f in fields]
    return [dict(zip(fields_name, p)) for p in parameters]


def transaction_management(func):
    @wraps(func)
    def _decorator(*args, **kwargs):
        if hasattr(transaction, "atomic"):
            with transaction.atomic(using=kwargs.get('using')):
                return func(*args, **kwargs)
        else:
            # Django < 1.6
            result = func(*args, **kwargs)
            transaction.commit_unless_managed(using=kwargs.get('using'))
            return result

    return _decorator


def _insert_many(model, objects, using="default", skip_result=True):
    if not objects:
        return

    con = connections[using]

    fields = _model_fields(model)
    parameters = [_prep_values(fields, o, con, True) for o in objects]

    table = model._meta.db_table
    col_names = ",".join(con.ops.quote_name(f.column) for f in fields)
    placeholders = ",".join(repeat("%s", len(fields)))

    sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, col_names, placeholders)
    con.cursor().executemany(sql, parameters)

    if not skip_result:
        return _build_rows(fields, parameters)

    return []


@transaction_management
def insert_many(model, objects, using="default", skip_result=True):
    '''
    Bulk insert list of Django objects. Objects must be of the same
    Django model.

    Note that save is not called and signals on the model are not
    raised.

    :param model: Django model class.
    :param objects: List of objects of class `model`.
    :param using: Database to use.

    '''

    return _insert_many(model, objects, using, skip_result)


def _update_many(model, objects, key_fields, value_fields,
                 using="default", skip_result=True):
    """Bulk update list of Django objects.

    Objects must be of the same Django model.

    :param model: Django model class.
    :param objects: List of objects of class `model`.
    :param key_fields: A list of field names to use in the WHERE clause.
    :param value_fields: A list of field names to update.
    :param using: Database to use.
    :param skip_result: don't return update rows. By default true.
    """
    if not objects:
        return

    con = connections[using]

    # Combine the fields for the parameter list
    param_fields = value_fields + key_fields
    parameters = [
        _prep_values(param_fields, o, con, False)
        for o in objects
    ]

    # Build the SQL
    table = model._meta.db_table
    assignments = ",".join(
        ("%s=%%s" % con.ops.quote_name(f.column))
        for f in value_fields
    )
    where_keys = " AND ".join(
        ("%s=%%s" % con.ops.quote_name(f.column))
        for f in key_fields
    )
    sql = "UPDATE %s SET %s WHERE %s" % (table, assignments, where_keys)
    con.cursor().executemany(sql, parameters)

    if not skip_result:
        return _build_rows(param_fields, parameters)

    return []


@transaction_management
def update_many(model, objects, keys=None, using="default", update_fields=None,
                exclude_fields=None):
    '''
    Bulk update list of Django objects. Objects must be of the same
    Django model.

    Note that save is not called and signals on the model are not
    raised.

    :param model: Django model class.
    :param objects: List of objects of class `model`.
    :param keys: An iterable of field names to use in the WHERE clause on. If
        none the model's primary key is used.
    :param using: Database to use.
    :param update_fields: An iterable of field names up be updated. If none
        or empty, all fields of the model are updated.
    :param exclude_fields: An iterable of field names to be excluded from
        the set of model fields to be updated.
    :raises ValueError: if keys is not None and is empty.
    '''

    key_fields, value_fields = _split_model_fields(
        model, keys, update_fields, exclude_fields
    )

    _update_many(model, objects, key_fields, value_fields, using)


def _filter_objects(con, objects, key_fields):
    '''Fitler out objects with duplicate key fields.'''
    keyset = set()

    # reverse = latest wins
    for o in reversed(objects):
        okeys = _prep_values(key_fields, o, con, False)
        if okeys in keyset:
            continue
        keyset.add(okeys)
        yield o


@transaction_management
def insert_or_update_many(model, objects, keys=None, using="default",
                          skip_update=False, update_fields=None,
                          exclude_fields=None):
    '''
    Bulk insert or update a list of Django objects. This works by
    first selecting each object's keys from the database. If an
    object's keys already exist, update, otherwise insert.

    Does not work with SQLite as it does not support tuple comparison.

    :param model: Django model class.
    :param objects: List of objects of class `model`.
    :param keys: An iterable of field names to use in the WHERE clause on. If
        none the model's primary key is used.
    :param using: Database to use.
    :param skip_update: Flag to insert only non-existing objects.
    :param update_fields: An iterable of field names to be updated. If none
        or empty, all fields of the model are updated.
    :param exclude_fields: An iterable of field names to be excluded from
        the set of model fields to be updated.
    :raises ValueError: if keys is not None and is empty.
    '''

    if not objects:
        return ([], [])

    con = connections[using]

    # Select key tuples from the database to find out which ones need to be
    # updated and which ones need to be inserted.

    key_fields, value_fields = _split_model_fields(
        model, keys, update_fields, exclude_fields
    )

    # Prepare field values before insert/update
    object_keys = [
        (o, _prep_values(key_fields, o, con, False))
        for o in objects
    ]
    parameters = [i for (_, k) in object_keys for i in k]

    table = model._meta.db_table
    col_names = ",".join(con.ops.quote_name(f.column) for f in key_fields)

    # repeat tuple values
    tuple_placeholder = "(%s)" % ",".join(repeat("%s", len(key_fields)))
    placeholders = ",".join(repeat(tuple_placeholder, len(objects)))

    sql = "SELECT %s FROM %s WHERE (%s) IN (%s)" % (
        col_names, table, col_names, placeholders)
    cursor = con.cursor()
    cursor.execute(sql, parameters)
    existing = set(cursor.fetchall())

    updated_rows = []
    if not skip_update:
        # Find the objects that need to be updated
        update_objects = [o for (o, k) in object_keys if k in existing]

        updated_rows = _update_many(
            model, update_objects,
            key_fields=key_fields,
            value_fields=value_fields,
            using=using,
            skip_result=False,
        )

    # Find the objects that need to be inserted.
    insert_objects = [o for (o, k) in object_keys if k not in existing]

    # Filter out any duplicates in the insertion
    filtered_objects = _filter_objects(con, insert_objects, key_fields)

    inserted_rows = _insert_many(model, filtered_objects, using=using,
                                 skip_result=False)

    return (inserted_rows, updated_rows)
