'''
Django bulk operations on simple models.
Does not attempt to cover all corner cases and related models.

Originally from http://people.iola.dk/olau/python/bulkops.py

'''
from itertools import repeat
from django.db import models, connections, transaction


def _model_fields(model, field_names=[]):
    fields = []
    for f in model._meta.fields:
        if (isinstance(f, models.AutoField) or
            (field_names and f.name not in field_names)):
                continue
        fields.append(f)
    return fields


def _prep_values(fields, obj, con, add):
    if hasattr(obj, 'presave') and callable(obj.presave):
        obj.presave()

    values = []
    for f in fields:
        field_type = f.get_internal_type()
        if field_type in ('DateTimeField', 'DateField', 'UUIDField'):
            values.append(f.pre_save(obj, add))
        else:
            values.append(f.get_db_prep_save(f.pre_save(obj, add), connection=con))
    return tuple(values)

def _build_rows(fields, parameters):
    fields_name = [f.name for f in fields]
    return [dict(zip(fields_name, p)) for p in parameters]


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
    inserted_rows = _insert_many(model, objects, using, skip_result)
    transaction.commit_unless_managed(using)
    return inserted_rows


def _update_many(model, objects, keys=None, using="default", skip_result=True,
        update_fields=[]):

    if not objects:
        return

    # If no keys specified, use the primary key by default
    keys = keys or [model._meta.pk.name]

    con = connections[using]

    # Split the fields into the fields we want to update and the fields we want
    # to update by in the WHERE clause.
    key_fields = [f for f in model._meta.fields if f.name in keys]
    value_fields = [f for f in _model_fields(model, update_fields) if f.name not in keys]

    assert key_fields, "Empty key fields"

    # Combine the fields for the parameter list
    param_fields = value_fields + key_fields
    parameters = [_prep_values(param_fields, o, con, False) for o in objects]

    # Build the SQL
    table = model._meta.db_table
    assignments = ",".join(("%s=%%s" % con.ops.quote_name(f.column))
                           for f in value_fields)
    where_keys = " AND ".join(("%s=%%s" % con.ops.quote_name(f.column))
                              for f in key_fields)
    sql = "UPDATE %s SET %s WHERE %s" % (table, assignments, where_keys)
    con.cursor().executemany(sql, parameters)

    if not skip_result:
        return _build_rows(param_fields, parameters)

    return []


def update_many(model, objects, keys=None, using="default", update_fields=[]):
    '''
    Bulk update list of Django objects. Objects must be of the same
    Django model.

    Note that save is not called and signals on the model are not
    raised.

    :param model: Django model class.
    :param objects: List of objects of class `model`.
    :param keys: A list of field names to update on.
    :param using: Database to use.
    :param update_fields: A list of fields up be updated. If empty, all fields
        of model are updated.

    '''
    _update_many(model, objects, keys, using, update_fields=update_fields)
    transaction.commit_unless_managed(using)


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


def insert_or_update_many(model, objects, keys=None, using="default",
    skip_update=False, update_fields=[]):
    '''
    Bulk insert or update a list of Django objects. This works by
    first selecting each object's keys from the database. If an
    object's keys already exist, update, otherwise insert.

    Does not work with SQLite as it does not support tuple comparison.

    :param model: Django model class.
    :param objects: List of objects of class `model`.
    :param keys: A list of field names to update on.
    :param using: Database to use.
    :param skip_update: Flag to insert only non-existing objects.
    :param update_fields: A list of fields up be updated. If empty, all fields
        of model are updated.

    '''
    if not objects:
        return ([], [])

    keys = keys or [model._meta.pk.name]
    con = connections[using]

    # Select key tuples from the database to find out which ones need to be
    # updated and which ones need to be inserted.
    key_fields = [f for f in model._meta.fields if f.name in keys]
    assert key_fields, "Empty key fields"

    object_keys = [(o, _prep_values(key_fields, o, con, False)) for o in objects]
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

        updated_rows = _update_many(model, update_objects, keys=keys,
            using=using, skip_result=False, update_fields=update_fields)

    # Find the objects that need to be inserted.
    insert_objects = [o for (o, k) in object_keys if k not in existing]

    # Filter out any duplicates in the insertion
    filtered_objects = _filter_objects(con, insert_objects, key_fields)

    inserted_rows = _insert_many(model, filtered_objects, using=using,
        skip_result=False)
    transaction.commit_unless_managed(using)
    return (inserted_rows, updated_rows)
