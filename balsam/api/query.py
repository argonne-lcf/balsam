REPR_OUTPUT_SIZE = 20


class Manager:
    model_class = None
    pk_field = "pk"
    bulk_create_enabled = True
    bulk_update_enabled = True

    @classmethod
    def register_client(cls, client_resource):
        """
        Link to a Client resource and make Manager
        accessible as `Model.objects`
        """
        cls.resource = client_resource
        cls.model_class.objects = cls

    @classmethod
    def all(cls):
        return Query(manager=cls)

    @classmethod
    def filter(cls, **kwargs):
        # TODO: kwargs should expand to filterable fields
        return Query(manager=cls).filter(**kwargs)

    @classmethod
    def get(cls, **kwargs):
        # TODO: kwargs should expand to filterable fields
        return Query(manager=cls).get(**kwargs)

    @classmethod
    def bulk_create(cls, instances):
        """Returns a list of newly created instances"""
        if not isinstance(instances, list):
            raise TypeError(
                f"instances must be a list of {cls.model_class.__name__} instances"
            )

        assert all(
            isinstance(obj, cls.model_class) for obj in instances
        ), f"bulk_create requires all items to be instances of {cls.model_class.__name__}"

        data_list = [cls._to_dict(obj) for obj in instances]
        response_data = cls.resource.bulk_create(data_list)
        return [cls._from_dict(dat) for dat in response_data]

    @classmethod
    def bulk_update(cls, instances, update_fields):
        """
        Perform a bulk patch of instances from the modified `instances` list and set of
        `update_fields`. Modifies the instances list in-place and returns None.
        """
        # TODO: validate update_fields
        data_list = [cls._to_dict(obj) for obj in instances]
        patch_list = [{key: d[key] for key in update_fields} for d in data_list]
        response_data = cls.resource.bulk_update_patch(patch_list)

        response_map = {item[cls.pk_field]: item for item in response_data}

        # Use response_map to update instances in-place
        for i, obj in enumerate(instances):
            pk = getattr(obj, cls.pk_field)
            updated_instance = cls._from_dict(response_map[pk])
            instances[i] = updated_instance

        return None

    @classmethod
    def _instance_update(cls, instance):
        response_data = cls._resource.update(
            uri=getattr(instance, cls.pk_field),
            payload=cls._to_dict(instance),
            partial=True,
        )
        for k, v in response_data.items():
            if k in instance.__fields__:
                setattr(instance, k, v)

    @classmethod
    def _to_dict(cls, instance):
        return instance.dict()

    @classmethod
    def _from_dict(cls, data):
        return cls.model_class.construct(**data)

    @classmethod
    def _unpack_list_response(cls, list_response):
        return [cls._from_dict(dat) for dat in list_response]

    @classmethod
    def _build_query_params(cls, filters, ordering, limit, offset):
        d = {}
        d.update(filters)
        if ordering:
            d.update(ordering=ordering)
        if limit:
            d.update(limit=limit)
        if offset:
            d.update(offset=offset)
        return d

    @classmethod
    def _get_list(cls, filters, ordering, limit, offset):
        instances = []
        count = 0
        query_params = cls._build_query_params(filters, ordering, limit, offset)
        response_data = cls.resource.list(**query_params)
        count = response_data["count"]
        instances = cls._unpack_list_response(response_data["results"])
        return instances, count

    @classmethod
    def _do_update_query(cls, patch, filters):
        query_params = cls._build_query_params(filters)
        response_data = cls.resource.bulk_update_query(patch, **query_params)
        return cls._unpack_list_response(response_data)

    @classmethod
    def _do_bulk_delete(cls, filters):
        query_params = cls._build_query_params(filters)
        response_data = cls.resource.bulk_destroy(**query_params)
        return response_data["deleted_count"]


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
        return self._limit is None or self._offset is None

    def _clone(self):
        clone = Query(manager=self._manager)
        clone._filters = self._filters.copy()
        clone._order_fields = self._order_fields.copy()
        clone._limit = self._limit
        clone._offset = self._offset
        return clone

    def _set_limits(self, start, stop):
        if start is None:
            start = 0
        self._offset = start
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
            return AttributeError("Cannot filter a sliced Query")
        clone = self._clone()
        clone._filters.update(kwargs)
        return clone

    def order_by(self, *fields):
        # TODO: should validate that only order-able fields are accepted
        if self.is_sliced:
            return AttributeError("Cannot re-order a sliced Query")
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

    def count(self):
        if self._count is None:
            _, count = self._manager._get_list(
                filters=self._filters, limit=0, offset=0,
            )
            self._count = count
        return self._count

    def update(self, **kwargs):
        # TODO: kwargs should expand to a set of allowed update_fields
        return self._manager._do_update_query(patch=kwargs, filters=self._filters)

    def delete(self):
        return self._manager._do_bulk_delete(filters=self._filters)
