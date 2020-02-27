import pathlib
from datetime import datetime
from .base_model import BalsamModel, BaseModel

REPR_OUTPUT_SIZE = 20


class ManagerMeta(type):
    def __new__(mcls, name, bases, attrs):
        super_new = super().__new__

        # Base Manager class created normally
        if not bases:
            return super_new(mcls, name, bases, attrs)

        # Subclasses double-checked for required class attrs; registered
        mcls.validate_manager_class(attrs)
        cls = super_new(mcls, name, bases, attrs)
        Manager._registry.append(cls)
        return cls

    def validate_manager_class(attrs):
        model_class = attrs["model_class"]
        assert issubclass(model_class, BalsamModel)


class Manager(metaclass=ManagerMeta):
    _registry = []
    model_class = None
    resource = None
    pk_field = "pk"
    bulk_create_enabled = False
    bulk_update_enabled = False
    bulk_delete_enabled = False

    def __init__(self, client_resource):
        """
        Link to a Client resource and make Manager
        accessible as `Model.objects`
        """
        self.resource = client_resource
        self.model_class.objects = self

    def __get__(self, instance, cls=None):
        if instance is not None:
            raise AttributeError(
                f"Manager isn't accessible via {cls.__name__} instances. "
                f"Access it via the class using `{cls.__name__}.objects`."
            )
        return self

    def all(self):
        return Query(manager=self)

    def count(self):
        return self.all().count()

    def first(self):
        return self.all().first()

    def filter(self, **kwargs):
        # TODO: kwargs should expand to filterable fields
        return Query(manager=self).filter(**kwargs)

    def get(self, **kwargs):
        # TODO: kwargs should expand to filterable fields
        return Query(manager=self).get(**kwargs)

    def create(self, **data):
        if self.bulk_create_enabled:
            instance = self.model_class(**data)
            created = self.bulk_create([instance])
            created = created[0]
        else:
            data = self._make_encodable(data)
            created = self.resource.create(**data)
            created = self._from_dict(created)
        return created

    def bulk_create(self, instances):
        """Returns a list of newly created instances"""
        if not self.bulk_create_enabled:
            raise NotImplementedError(
                "The {self.model_class.__name__} API does not offer bulk_create"
            )

        if not isinstance(instances, list):
            raise TypeError(
                f"instances must be a list of {self.model_class.__name__} instances"
            )

        assert all(
            isinstance(obj, self.model_class) for obj in instances
        ), f"bulk_create requires all items to be instances of {self.model_class.__name__}"

        data_list = [self._to_dict(obj) for obj in instances]
        response_data = self.resource.bulk_create(data_list)
        return [self._from_dict(dat) for dat in response_data]

    def bulk_update(self, instances, update_fields):
        """
        Perform a bulk patch of instances from the modified `instances` list and set of
        `update_fields`. Modifies the instances list in-place and returns None.
        """
        # TODO: validate update_fields
        if not self.bulk_update_enabled:
            raise NotImplementedError(
                "The {self.model_class.__name__} API does not offer bulk_update"
            )

        data_list = [self._to_dict(obj) for obj in instances]
        patch_list = [{key: d[key] for key in update_fields} for d in data_list]
        response_data = self.resource.bulk_update_patch(patch_list)

        response_map = {item[self.pk_field]: item for item in response_data}

        # Use response_map to update instances in-place
        for i, obj in enumerate(instances):
            pk = getattr(obj, self.pk_field)
            updated_instance = self._from_dict(response_map[pk])
            instances[i] = updated_instance

        return None

    def _to_dict(self, instance):
        d = instance.dict()
        return self._make_encodable(d)

    def _make_encodable(self, data):
        if isinstance(data, BaseModel):
            return self._make_encodable(data.dict())
        elif isinstance(data, dict):
            return {k: self._make_encodable(v) for k, v in data.items()}
        elif isinstance(data, list):
            return list(map(self._make_encodable, data))
        elif isinstance(data, tuple):
            return tuple(map(self._make_encodable, data))
        elif isinstance(data, pathlib.Path):
            return data.as_posix()
        elif isinstance(data, datetime):
            return data.isoformat()
        else:
            return data

    def _from_dict(self, data):
        return self.model_class(**data)

    def _unpack_list_response(self, list_response):
        return [self._from_dict(dat) for dat in list_response]

    def _build_query_params(self, filters, ordering=None, limit=None, offset=None):
        d = {}
        d.update(filters)
        if ordering is not None:
            d.update(ordering=ordering)
        if limit is not None:
            d.update(limit=limit)
        if offset is not None:
            d.update(offset=offset)
        return d

    def _get_list(self, filters, ordering, limit, offset):
        instances = []
        count = 0
        query_params = self._build_query_params(filters, ordering, limit, offset)
        response_data = self.resource.list(**query_params)
        count = response_data["count"]
        instances = self._unpack_list_response(response_data["results"])
        return instances, count

    def _do_update(self, instance):
        response_data = self.resource.update(
            uri=getattr(instance, self.pk_field),
            payload=self._to_dict(instance),
            partial=True,
        )
        for k, v in response_data.items():
            if k in instance.__fields__:
                setattr(instance, k, v)

    def _do_bulk_update_query(self, patch, filters):
        if not self.bulk_update_enabled:
            raise NotImplementedError(
                "The {self.model_class.__name__} API does not offer bulk updates"
            )

        query_params = self._build_query_params(filters)
        response_data = self.resource.bulk_update_query(patch, **query_params)
        return self._unpack_list_response(response_data)

    def _do_delete(self, instance):
        self.resource.destroy(uri=getattr(instance, self.pk_field))

    def _do_bulk_delete(self, filters):
        if not self.bulk_delete_enabled:
            raise NotImplementedError(
                "The {self.model_class.__name__} API does not offer bulk deletes"
            )

        query_params = self._build_query_params(filters)
        response_data = self.resource.bulk_destroy(**query_params)
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
