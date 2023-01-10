import wrapt


@wrapt.decorator
def synchronized(wrapped, instance, args, kwargs):
    instance = instance or args[0]
    lock = getattr(instance, "_lock")
    with lock:
        return wrapped(*args, **kwargs)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
