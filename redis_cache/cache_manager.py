import cPickle
from functools import wraps
import inspect

from .connection import get_current_connection


class CacheManager(object):
    """This is the base class for cache managers.

    *key_base* is used to prefix cache keys generated by this instance.

    *ttl* is cache key TTL in seconds. If it is omitted (or *None*) then keys
    stored by this instance won't expire.

    *connection* allows binding the instance to an explicit Redis connection.
    If it is omitted global connection defined with
    :func:`redcache.connection.use_connection` will be used.

    If no connection is defined then no caching will happen."""

    def __init__(self, key_base=u'cache', ttl=None, connection=None):
        self._key_base = key_base
        self._ttl = ttl
        self._connection = connection

    @property
    def connection(self):
        """Connection property."""
        if self._connection:
            return self._connection
        else:
            return get_current_connection()

    def key(self, f, args):
        """Key generator for function *f* with positional arguments *args*.

        Example key: ``key_base:func_name:arg1:arg2``.

        **Instance and class methods**

        If the first argument of *f* is either *self* or *cls* it won't be used
        while creating the key."""
        f_args = inspect.getargspec(f).args

        nameparts = [f.__name__]
        if inspect.ismethod(f):
            f_class = None
            if inspect.isclass(f.im_self):
                f_class = f.im_self  # f is a class method
            else:
                f_class = f.im_self.__class__  # f is an instance method

            nameparts = [f_class.__name__] + nameparts

        argparts = [self._key_base, '.'.join(nameparts)]
        if args and len(f_args) > 0:
            idx = 0
            if f_args[0] in ('cls', 'self'):
                idx = 1

            for arg in args[idx:]:
                argparts.append(unicode(arg))

        key = u':'.join(argparts)
        return key

    def after_load(self, data, f_args=None, f_kwargs=None):
        """Process and return *data* after loading it from Redis. *f_args* and
        *f_kwargs* contain positional and keywords args passed to decorated
        function.

        Default implementation uses cPickle to unserialize data."""
        return cPickle.loads(data)

    def before_save(self, data, f_args=None, f_kwargs=None):
        """Process and return *data* before saving it to Redis. *f_args* and
        *f_kwargs* contain positional and keywords args passed to decorated
        function.

        Default implementation uses cPickle to unserialize data."""
        return cPickle.dumps(data)

    def load(self, key, f_args=None, f_kwargs=None):
        """Load data for *key* from Redis. *f_args* and *f_kwargs* contain
        positional and keywords args passed to decorated function.

        Default implementation uses GET command."""
        return self.connection.get(key)

    def save(self, key, data, f_args=None, f_kwargs=None):
        """Save data into Redis *key*. *f_args* and *f_kwargs* contain
        positional and keywords args passed to decorated function.

        Default implementation uses SET command."""
        self.connection.set(key, data)

    def cache(self, f):
        """Decorate *f* function to enable caching it.

        If the function returns *None* then it won't be cached."""
        @wraps(f)
        def wrapper(*args, **kwargs):
            key = self.key(f, args)

            data = None
            if self.connection:
                data = self.load(key, f_args=args, f_kwargs=kwargs)

            if data:
                data = self.after_load(data, f_args=args, f_kwargs=kwargs)
            else:
                data = f(*args, **kwargs)

                if data is not None:
                    cached = self.before_save(data, f_args=args,
                                              f_kwargs=kwargs)

                    if self.connection:
                        self.save(key, cached, f_args=args, f_kwargs=kwargs)

                        if self._ttl:
                            self.connection.expire(key, self._ttl)

            return data

        return wrapper


class DefaultCacheManager(CacheManager):
    """This is default cache manager for simple caching of generic
    functions.

    Basically it's equivalent to :py:class:`CacheManager` with default
    settings."""

    def cache(self, *args, **kwargs):
        """Decorate *f* function to enable caching it.

        Use *ttl* keyword arg to override default infinite TTL.

        If the function returns *None* then it won't be cached."""
        ttl = kwargs.get('ttl', self._ttl)

        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                key = self.key(f, args)

                data = None
                if self.connection:
                    data = self.load(key, f_args=args, f_kwargs=kwargs)

                if data:
                    data = self.after_load(data, f_args=args, f_kwargs=kwargs)
                else:
                    data = f(*args, **kwargs)

                    if data is not None:
                        cached = self.before_save(data, f_args=args,
                                                  f_kwargs=kwargs)

                        if self.connection:
                            self.save(key, cached, f_args=args,
                                      f_kwargs=kwargs)

                            if ttl:
                                self.connection.expire(key, ttl)

                return data

            return wrapper

        if args:
            return decorator(args[0])
        else:
            return decorator

default_cache = DefaultCacheManager()
