REPR_OUTPUT_SIZE = 20


class Query:
    def __init__(self, manager):
        self._manager = manager
        self._result_cache = None
        self._filters = {}
        self._order_fields = []
        self._count = None
        self._limit = None
        self._offset = None

    def __get__(self, instance, cls=None):
        if instance is not None:
            raise AttributeError(
                "Query isn't accessible via %s instances" % cls.__name__
            )
        return self

    def __repr__(self):
        data = list(self[: REPR_OUTPUT_SIZE + 1])
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return "<%s %r>" % (self.__class__.__name__, data)

    def __len__(self):
        self._fetch_cache()
        return len(self._result_cache)

    def __bool__(self):
        self._fetch_cache()
        return bool(self._result_cache)

    def __setitem__(self, k, v):
        if not isinstance(k, int):
            raise TypeError("Item assignment only support for int index")

        if self._result_cache is None:
            self._fetch_cache()

        self._result_cache[k] = v

    def __getitem__(self, k):
        """
        Retrieve an item or slice from the set of results.
        """
        if not isinstance(k, (int, slice)):
            raise TypeError(
                "Query indices must be integers or slices, not %s." % type(k).__name__
            )
        assert (not isinstance(k, slice) and (k >= 0)) or (
            isinstance(k, slice)
            and (k.start is None or k.start >= 0)
            and (k.stop is None or k.stop >= 0)
        ), "Negative indexing is not supported."

        if self._result_cache is not None:
            return self._result_cache[k]

        if isinstance(k, slice):
            clone = self._clone()
            if k.start is not None:
                start = int(k.start)
            else:
                start = None
            if k.stop is not None:
                stop = int(k.stop)
            else:
                stop = None
            clone._set_limits(start, stop)
            return list(clone)[:: k.step] if k.step else clone
        else:
            clone = self._clone()
            clone._set_limits(k, k + 1)
            clone._fetch_cache()
            return clone._result_cache[0]

    @property
    def is_sliced(self):
        return not (self._limit is None and self._offset is None)

    def _clone(self):
        clone = Query(manager=self._manager)
        clone._filters = self._filters.copy()
        clone._order_fields = self._order_fields
        clone._limit = self._limit
        clone._offset = self._offset
        return clone

    def _set_limits(self, start, stop):
        if start is None:
            start = 0
        self._offset = start
        if stop is not None:
            self._limit = stop - start

    def __iter__(self):
        self._fetch_cache()
        return iter(self._result_cache)

    def _fetch_cache(self):
        if self._result_cache is not None:
            return

        instances, count = self._manager._get_list(
            filters=self._filters,
            ordering=self._order_fields,
            limit=self._limit,
            offset=self._offset,
        )
        if count is not None:
            self._count = count
        self._result_cache = instances

    def filter(self, **kwargs):
        # TODO: kwargs should expand to the set of filterable model fields
        if self.is_sliced:
            raise AttributeError("Cannot filter a sliced Query")
        clone = self._clone()

        for key, val in kwargs.items():
            if isinstance(val, dict):
                kwargs[key] = [f"{k}:{v}" for k, v in val.items()]
        clone._filters.update(kwargs)
        return clone

    def order_by(self, *fields):
        # TODO: should validate that only order-able fields are accepted
        if self.is_sliced:
            raise AttributeError("Cannot re-order a sliced Query")
        clone = self._clone()
        clone._order_fields = tuple(fields)
        return clone

    # Methods that do not return a Query
    # **********************************
    def get(self, **kwargs):
        # TODO: kwargs should expand to the set of filterable model fields
        clone = self.filter(**kwargs)
        results = list(clone)
        nobj = len(results)
        if nobj == 1:
            return results[0]
        elif nobj == 0:
            raise self._manager.model_class.DoesNotExist
        else:
            raise self._manager.model_class.MultipleObjectsReturned(nobj)

    def first(self):
        return self[0]

    def count(self):
        if self._count is None:
            _, _count = self._manager._get_list(
                filters=self._filters, limit=0, offset=0, ordering=None
            )
            self._count = _count
        return self._count

    def update(self, **kwargs):
        # TODO: kwargs should expand to a set of allowed update_fields
        return self._manager._do_bulk_update_query(patch=kwargs, filters=self._filters)

    def delete(self):
        return self._manager._do_bulk_delete(filters=self._filters)
