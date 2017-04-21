db_settings="# CircleCI DB settings.

DATABASES = {}
DATABASES['default'] = {}
DATABASES['default']['ENGINE'] = 'django.db.backends.postgresql_psycopg2'
DATABASES['default']['NAME'] = 'circle_test'
DATABASES['default']['USER'] = 'ubuntu'
CACHES = {}
CACHES['default'] = {}
CACHES['default']['BACKEND'] = 'django.core.cache.backends.dummy.DummyCache'"

cp bulktest/test_settings.py bulktest/test_settings_circle.py
echo "$db_settings" >> bulktest/test_settings_circle.py
