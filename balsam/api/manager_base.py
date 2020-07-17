from .query import Query


class Manager:
    model_class = None
    bulk_create_enabled = False
    bulk_update_enabled = False
    bulk_delete_enabled = False
    paginated_list_response = True
    path = ""
    _client = None

    @staticmethod
    def set_client(client):
        Manager._client = client

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
        # We want to pass through the BalsamModel constructor for validation
        instance = self.model_class(**data)
        if self.bulk_create_enabled:
            created = self.bulk_create([instance])
            created = created[0]
        else:
            data = instance._create_model.dict()
            created = self._client.post(self.path, **data)
            created = self.model_class.from_api(created)
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
            isinstance(obj._create_model, self.model_class.create_model_cls)
            for obj in instances
        ), f"bulk_create requires all items to be instances of {self.model_class.__name__}"

        data_list = [obj._create_model.dict() for obj in instances]
        response_data = self._client.bulk_post(self.path, data_list)
        # Cannot update in-place: no IDs to perform the mapping yet
        return [self.model_class.from_api(dat) for dat in response_data]

    def bulk_update(self, instances):
        """
        Perform a bulk patch of instances from the modified `instances` list and set of
        `update_fields`. Modifies the instances list in-place and returns None.
        """
        # TODO: validate update_fields
        if not self.bulk_update_enabled:
            raise NotImplementedError(
                "The {self.model_class.__name__} API does not offer bulk_update"
            )

        patch_list = [
            {"id": obj.id, **obj._update_model.dict(exclude_unset=True)}
            for obj in instances
            if hasattr(obj, "_update_model")
        ]
        response_data = self._client.bulk_patch(self.path, patch_list)
        response_map = {item["id"]: item for item in response_data}
        # Use response_map to update instances in-place
        for obj in instances:
            obj._refresh_from_dict(response_map[obj.id])

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
        query_params = self._build_query_params(filters, ordering, limit, offset)
        response_data = self._client.get(self.path, **query_params)
        if self.paginated_list_response:
            count = response_data["count"]
            results = response_data["results"]
        else:
            count = None
            results = response_data
        instances = [self.model_class.from_api(dat) for dat in results]
        return instances, count

    def _do_update(self, instance):
        response_data = self._client.put(
            self.path + f"{instance.id}",
            **instance._update_model.dict(exclude_unset=True),
        )
        instance._refresh_from_dict(response_data)

    def _do_bulk_update_query(self, patch, filters):
        if not self.bulk_update_enabled:
            raise NotImplementedError(
                f"The {self.model_class.__name__} API does not offer bulk updates"
            )
        query_params = self._build_query_params(filters)
        response_data = self._client.bulk_put(self.path, patch, **query_params)
        instances = [self.model_class.from_api(dat) for dat in response_data]
        return instances

    def _do_delete(self, instance):
        self._client.delete(self.path + f"{instance.id}")
        instance._read_model.id = None

    def _do_bulk_delete(self, filters):
        if not self.bulk_delete_enabled:
            raise NotImplementedError(
                f"The {self.model_class.__name__} API does not offer bulk deletes"
            )
        query_params = self._build_query_params(filters)
        self._client.bulk_delete(self.path, **query_params)
