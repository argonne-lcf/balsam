import json
import yaml


class BalsamModelField:
    def __init__(self, name):
        self.name = name

    def __get__(self, obj, type=None):
        if obj._state == "clean":
            if hasattr(obj._read_model, self.name):
                return getattr(obj._read_model, self.name)
            elif obj.update_model_cls and self.name in obj.update_model_cls.__fields__:
                return None
            else:
                raise AttributeError(f"Cannot access Field {self.name}")
        elif obj._state == "creating":
            if hasattr(obj._create_model, self.name):
                return getattr(obj._create_model, self.name)
            elif self.name in obj.read_model_cls.__fields__:
                return None
            else:
                raise AttributeError(f"Cannot access Field {self.name}")
        else:
            if self.name in obj._dirty_fields:
                return getattr(obj._update_model, self.name)
            elif hasattr(obj._read_model, self.name):
                return getattr(obj._read_model, self.name)
            elif obj.update_model_cls and self.name in obj.update_model_cls.__fields__:
                return None
            else:
                raise AttributeError(f"Cannot access Field {self.name}")

    def __set__(self, obj, value):
        if obj._state == "creating":
            if self.name not in obj._create_model.__fields__:
                raise AttributeError(
                    f"Cannot set {self.name} when creating {obj._modelname}"
                )
            setattr(obj._create_model, self.name, value)
        else:
            if obj._update_model is None:
                obj._update_model = obj.update_model_cls()
            if self.name not in obj._update_model.__fields__:
                raise AttributeError(
                    f"Cannot set {self.name} when updating {obj._modelname}"
                )
            setattr(obj._update_model, self.name, value)
            obj._dirty_fields.add(self.name)
            obj._state = "dirty"


class BalsamModelMeta(type):
    def __new__(mcls, name, bases, attrs):
        if bases == (object,) or bases == ():
            return super().__new__(mcls, name, bases, attrs)

        field_names = set()
        for model_cls in [
            attrs["create_model_cls"],
            attrs["update_model_cls"],
            attrs["read_model_cls"],
        ]:
            if model_cls is not None:
                field_names.update(model_cls.__fields__)
        for field_name in field_names:
            field = BalsamModelField(field_name)
            attrs[field_name] = field
        cls = super().__new__(mcls, name, bases, attrs)
        return cls


class BalsamModel(metaclass=BalsamModelMeta):
    create_model_cls = None
    update_model_cls = None
    read_model_cls = None
    objects = None

    def __init__(self, _api_data=False, **kwargs):
        self._create_model = None
        self._update_model = None
        self._read_model = None
        self._state = None
        self._dirty_fields = set()

        if _api_data:
            self._read_model = self.read_model_cls(**kwargs)
            self._state = "clean"
        else:
            if self.create_model_cls is None:
                raise ValueError(f"{self._modelname} is read only")
            self._create_model = self.create_model_cls(**kwargs)
            self._state = "creating"

    def _set_clean(self):
        self._state = "clean"
        self._dirty_fields.clear()
        self._update_model = None

    @property
    def _modelname(self):
        return self.__class__.__name__

    @classmethod
    def from_api(cls, data):
        return cls(_api_data=True, **data)

    def _refresh_from_dict(self, data):
        self._read_model = self.read_model_cls(**data)
        self._set_clean()

    def save(self):
        if self._state == "dirty":
            self.__class__.objects._do_update(self)
        elif self._state == "creating":
            created = self.__class__.objects.create(**self._create_model.dict())
            self._refresh_from_dict(created._read_model.dict())
            self._create_model = None

    def refresh_from_db(self):
        if self._state == "creating":
            raise AttributeError("Cannot refresh instance before it's saved")
        from_db = self.__class__.objects.get(id=self.id)
        self._read_model = from_db._read_model.copy()
        self._set_clean()

    def delete(self):
        if self._state == "creating":
            raise AttributeError("Cannot delete instance that hasn't been saved")
        self.__class__.objects._do_delete(self)

    class DoesNotExist(Exception):
        pass

    class MultipleObjectsReturned(Exception):
        def __init__(self, nobj):
            super().__init__(f"Returned {nobj} objects; expected one!")

    def display_model(self):
        if self._state == "creating":
            return self._create_model
        elif self._state == "dirty":
            return self._read_model.copy(
                update=self._update_model.dict(exclude_unset=True)
            )
        else:
            return self._read_model

    def display_dict(self):
        """Prettify through smart JSON serializer"""
        return json.loads(self.display_model().json())

    def __repr__(self):
        args = ", ".join(f"{k}={v}" for k, v in self.display_dict().items())
        return f"{self._modelname}({args})"

    def __str__(self):
        d = self.display_dict()
        return yaml.dump(d, sort_keys=False, indent=4)

    def __eq__(self, other):
        if not isinstance(other, BalsamModel):
            return False
        return (
            self._state == "clean"
            and other._state == "clean"
            and self._read_model == other._read_model
        )
