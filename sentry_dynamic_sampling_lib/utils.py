import wrapt


@wrapt.decorator
def synchronized(wrapped, instance, args, kwargs):
    instance = instance or args[0]
    lock = getattr(instance, "_lock")
    with lock:
        return wrapped(*args, **kwargs)
