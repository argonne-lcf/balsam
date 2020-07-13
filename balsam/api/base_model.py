from pydantic import BaseModel
from typing import Optional


class BalsamModelField:
    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, type=None):
        if obj._state == "clean":
            return getattr(obj._read_model, self.name)
        elif obj._state == "creating":
            return getattr(obj._create_model, self.name)
        else:
            if self.name in obj._dirty_fields:
                return getattr(obj._update_model, self.name)
            else:
                return getattr(obj._read_model, self.name)

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
        cls = super().__new__(mcls, name, bases, attrs)
        if bases == (object,) or bases == ():
            return cls

        field_names = set()
        for model_cls in [
            cls.create_model_cls,
            cls.update_model_cls,
            cls.read_model_cls,
        ]:
            if model_cls is not None:
                field_names.update(model_cls.__fields__)
        for field_name in field_names:
            field = BalsamModelField()
            setattr(cls, field_name, field)
            field.__set_name__(cls, field_name)
        cls.objects.model_class = cls
        return cls


class BalsamModel(metaclass=BalsamModelMeta):
    create_model_cls = None
    update_model_cls = None
    read_model_cls = None
    objects = None

    def __init__(self, **kwargs):
        self._create_model = None
        self._update_model = None
        self._read_model = None
        self._state = None
        self._dirty_fields = set()

        if "_api_data" in kwargs:
            self._read_model = self.read_model_cls(**kwargs["_api_data"])
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
    def from_api(cls, **data):
        return cls(_api_data=data)

    def save(self):
        if self._state == "dirty":
            self.__class__.objects._do_update(self)
        elif self._state == "creating":
            created = self.__class__.objects.create(**self._create_model.dict())
            self._read_model = created._read_model.copy()
            self._create_model = None
        self._set_clean()

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


class FauxManager:
    def _do_update(self, instance):
        updates = instance._update_model.dict(exclude_unset=True)
        new = instance._read_model.copy(update=updates)
        instance._read_model = new
        print(f"Doing update of {instance._modelname} {instance.id}: {updates}")

    def _do_delete(self, instance):
        print(f"DELETE {instance._modelname} {instance.id}")
        instance.id = None

    def create(self, **kwargs):
        print("POSTing new instance")
        return self.model_class.from_api(**kwargs, id=123)

    def get(self, id):
        print(f"GET {self.model_class._modelname} {id}")
        return self.model_class.from_api(id=id, workdir="/foo/bar", args="blah")


class JobCreate(BaseModel):
    workdir: str
    args: str


class JobRead(BaseModel):
    id: int
    workdir: str
    args: str


class JobUpdate(BaseModel):
    workdir: Optional[str] = None
    args: Optional[str] = None


class Job(BalsamModel):
    objects = FauxManager()
    create_model_cls = JobCreate
    update_model_cls = JobUpdate
    read_model_cls = JobRead
