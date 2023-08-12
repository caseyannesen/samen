from dogpile.cache import make_region
import os 


cache_path = "/var/lib/samen/cache.dbm"

if not os.path.exists(os.path.dirname(cache_path)):
    os.makedirs(os.path.dirname(cache_path))

cache = make_region().configure(
    'dogpile.cache.dbm',
    expiration_time=3600,
    arguments={'filename': cache_path}
)

# cache.set('my_key', 'my_value', expiration_time=60)
# my_value = cache.get('my_key')