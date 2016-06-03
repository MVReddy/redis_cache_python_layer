'''
Created on May 11, 2016

@author: Venkata Reddy Mulam
'''
import logging
from importlib import import_module

import redis
from django.conf import settings

logger = logging.getLogger(getattr(settings, 'LOGGER_NAME', ''))
timeout = getattr(settings, 'DEFAULT_CACHE_TIMEOUT', None)


class CacheUpdateManager(object):
    '''
    Object with pre-defined (dynamic changes are not allowed) Managers as its' properties
    the keys that get passed in in the init are the only ones for this Update Manager
    '''

    def __init__(self, keys, cache):
        managers = {key: CacheManager(key, cache, settings.CACHE_KEYS[key]) for key in keys}
        self.__dict__.update(managers)

    def add_to_manager(self, namespace, ignore_args, view, func, *args, **kwargs):
        manager = self.get(namespace)
        if isinstance(manager, CacheManager):
            logger.debug('Adding {0} to {1}'.format(func, manager))
#             func_str = '{0}.{1}'.format(func.__module__, func.__name__)
            manager.append((func, ignore_args, view, namespace, args, kwargs))
#             manager.cache.set(namespace, manager, timeout)
            return True
        return False

    def update_namespace(self, namespace):
        manager = self.get(namespace)
        if isinstance(manager, CacheManager):
            return manager.update(self)

        raise AttributeError('No Manager for namespace: {}'.format(namespace))

    def update_all(self):
        result = []
        for namespace in self.namespaces:
            result.append(self.update_namespace(namespace))
        return result

    def flush_app(self, app_name, cache):
        '''
        flushes all keys for the given app_name
        '''
        pattern = '*.{0}.*'.format(app_name)
        if isinstance(cache, redis.StrictRedis):
            keys = cache.keys(pattern)
            return cache.delete(*keys)
        else:
            return cache.delete_pattern(pattern)

    @property
    def namespaces(self):
        return self.__dict__.keys()

    def __getattr__(self, name):
        if name in self.__dict__:
            return name
        raise AttributeError

    def get(self, name, default=None):
        return self.__dict__.get(name, default)

    def __setattr__(self, name, value):
        raise AttributeError

    def __delattr__(self, *args, **kwargs):
        raise AttributeError

    @staticmethod
    def get_cache_update_manager():
        from cache_utils.decorators import cache_update_manager
        return cache_update_manager


class CacheManager(list):
    '''
    A (glorified) list of functions and args which gets updated via the UpdateManager
    '''
    views = list()

    def __init__(self, namespace, cache, app_name):
        self.namespace = namespace
        self.cache = cache
        self.app_name = app_name

    def append(self, tup):
        if isinstance(tup, tuple) and len(tup) == 6:
            if tup[2]:
                CacheManager.views.append(tup)

            return list.append(self, tup)
        raise ValueError('CacheKey only takes objects of type: tuple, with (func, ignore_args, args, kwargs)')

    def update(self, update_manager):
        '''
        reruns and resets the cache for all funcs in this Manager
        '''
        from cache_utils.decorators import cache_create_key
        logger.debug('Updating CacheManager - {0}'.format(self))
        num_updated = 0
        for func, ignore_args, view, namespace, args, kwargs in self:
            try:
                # ===============================================================
                # import_module(func.split('.')[0])
                # real_func = eval(func)
                # ===============================================================
                result = func(*args, **kwargs)
            except Exception as e:
                logger.error('Cache Update for {0} failed for {1} '.format(self, func), exc_info=True)
                continue
            key = cache_create_key(self.namespace, ignore_args, func.__name__, *args, **kwargs)
            self.cache.set(key, result, timeout)

            logger.debug('Updated {0}'.format(func))
            num_updated += 1
        update_manager.flush_app(self.app_name, self.cache)

        # Re caching all Views
        for func, ignore_args, view, namespace, args, kwargs in CacheManager.views:
            msg = "Updating {0}({1}{2})".format(func, args, kwargs)
            print msg
            view_key = cache_create_key(namespace, ignore_args, func.__name__, *args, **kwargs)
            self.cache.delete(view_key)
            func(*args, **kwargs)

        logger.debug('Finished updating CacheManager - {0}'.format(self))
        return num_updated

    def flush(self):
        ''' flushes all keys for this namespace'''
        pattern = '*{0}*'.format(self.namespace)
        return self.cache.delete_pattern(pattern)

    @property
    def list(self):
        return self.__repr__()

    def __str__(self, *args, **kwargs):
        return '{} : {}'.format(self.app_name, self.namespace)
