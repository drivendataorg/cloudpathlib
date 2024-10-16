class class_or_instancemethod(classmethod):
    """Allow different behavior depending on if called on instance or class."""

    def __get__(self, instance, type_):
        descr_get = super().__get__ if instance is None else self.__func__.__get__
        return descr_get(instance, type_)
