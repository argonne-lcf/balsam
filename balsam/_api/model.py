import json
from typing import TYPE_CHECKING, Any, Dict, Generic, Optional, Set, Tuple, Type, TypeVar, cast

import yaml
from pydantic import BaseModel

if TYPE_CHECKING:
    from .manager import Manager

F = TypeVar("F")


class Field(Generic[F]):
    name: str

    def __get__(self, obj: "BalsamModel", type: "Optional[Type[BalsamModel]]" = None) -> F:
        if obj._state == "clean":
            # The field is clean and readable (fetched from REST API):
            if hasattr(obj._read_model, self.name):
                return getattr(obj._read_model, self.name)  # type: ignore
            # The field is write-only but not yet written to:
            elif obj._update_model_cls and self.name in obj._update_model_cls.__fields__:
                return cast(F, None)
            else:
                raise AttributeError(f"Cannot access Field {self.name}")
        elif obj._state == "creating":
            # The field is writeable at object creation time (not yet sent to REST API):
            if hasattr(obj._create_model, self.name):
                return getattr(obj._create_model, self.name)  # type: ignore
            # The field is readable but not yet established by creation:
            elif self.name in obj._read_model_cls.__fields__:
                return cast(F, None)
            else:
                raise AttributeError(f"Cannot access Field {self.name}")
        else:
            # The field has been mutated locally:
            if self.name in obj._dirty_fields:
                return getattr(obj._update_model, self.name)  # type: ignore
            # The field is clean and readable (fetched from REST API):
            elif hasattr(obj._read_model, self.name):
                return getattr(obj._read_model, self.name)  # type: ignore
            # The field is write-only but not yet written to:
            elif obj._update_model_cls and self.name in obj._update_model_cls.__fields__:
                return cast(F, None)
            else:
                raise AttributeError(f"Cannot access Field {self.name}")

    def __set__(self, obj: "BalsamModel", value: F) -> None:
        if obj._state == "creating":
            if self.name not in obj._create_model.__fields__:  # type: ignore
                raise AttributeError(f"Cannot set {self.name} when creating {obj._modelname}")
            setattr(obj._create_model, self.name, value)
        else:
            if obj._update_model_cls is None:
                raise AttributeError(f"{obj.__class__.__name__} is read-only")
            if obj._update_model is None:
                obj._update_model = obj._update_model_cls()
            if self.name not in obj._update_model.__fields__:
                raise AttributeError(f"Cannot set {self.name} when updating {obj._modelname}")
            setattr(obj._update_model, self.name, value)
            obj._dirty_fields.add(self.name)
            obj._state = "dirty"


T = TypeVar("T", bound="BalsamModel")


class BalsamModelMeta(type):
    def __new__(mcls, name: str, bases: Tuple[Any, ...], attrs: Dict[str, Any]) -> "BalsamModelMeta":
        if bases == (object,) or bases == () or "_read_model_cls" not in attrs:
            return super().__new__(mcls, name, bases, attrs)

        field_names = set()
        for model_cls in [
            attrs["_create_model_cls"],
            attrs["_update_model_cls"],
            attrs["_read_model_cls"],
        ]:
            if model_cls is not None:
                field_names.update(model_cls.__fields__)
        for field_name in field_names:
            if field_name in attrs:
                attrs[field_name].name = field_name
        cls = super().__new__(mcls, name, bases, attrs)
        return cls


class BalsamModel(metaclass=BalsamModelMeta):
    _create_model_cls: Optional[Type[BaseModel]]
    _update_model_cls: Optional[Type[BaseModel]]
    _read_model_cls: Type[BaseModel]
    _create_model: Optional[BaseModel]
    _update_model: Optional[BaseModel]
    _read_model: Optional[BaseModel]
    objects: "Manager"  # type: ignore
    id: Any

    def __init__(self, _api_data: bool = False, **kwargs: Any) -> None:
        self._create_model = None
        self._update_model = None
        self._read_model = None
        self._state = None
        self._dirty_fields: Set[str] = set()

        if _api_data:
            self._read_model = self._read_model_cls(**kwargs)
            self._state = "clean"
        else:
            if self._create_model_cls is None:
                raise ValueError(f"{self._modelname} is read only")
            self._create_model = self._create_model_cls(**kwargs)
            self._state = "creating"

    def _set_clean(self) -> None:
        self._state = "clean"
        self._dirty_fields.clear()
        self._update_model = None

    @property
    def _modelname(self) -> str:
        return self.__class__.__name__

    @classmethod
    def _from_api(cls: Type[T], data: Any) -> T:
        return cls(_api_data=True, **data)

    def _refresh_from_dict(self, data: Dict[Any, Any]) -> None:
        self._read_model = self._read_model_cls(**data)
        self._set_clean()

    def save(self) -> None:
        if self._state == "dirty":
            self.__class__.objects._do_update(self)
        elif self._state == "creating":
            assert self._create_model is not None
            created = self.__class__.objects._create(**self._create_model.dict())
            assert created._read_model is not None
            self._refresh_from_dict(created._read_model.dict())
            self._create_model = None

    def refresh_from_db(self) -> None:
        if self._state == "creating":
            raise AttributeError("Cannot refresh instance before it's saved")
        from_db = self.__class__.objects.all()._get(id=self.id)
        assert from_db._read_model is not None
        self._read_model = from_db._read_model.copy()
        self._set_clean()

    def delete(self) -> None:
        if self._state == "creating":
            raise AttributeError("Cannot delete instance that hasn't been saved")
        self.__class__.objects._do_delete(self)

    class DoesNotExist(Exception):
        def __init__(self, filters: Dict[str, Any]) -> None:
            super().__init__(f"No results matched the query params: {filters}")

    class MultipleObjectsReturned(Exception):
        def __init__(self, nobj: int) -> None:
            super().__init__(f"Returned {nobj} objects; expected one!")

    def display_model(self) -> BaseModel:
        if self._state == "creating":
            assert self._create_model is not None
            return self._create_model
        elif self._state == "dirty":
            assert self._read_model is not None and self._update_model is not None
            return self._read_model.copy(update=self._update_model.dict(exclude_unset=True))
        else:
            assert self._read_model is not None
            return self._read_model

    def display_dict(self) -> Any:
        """Prettify through smart JSON serializer"""
        return json.loads(self.display_model().json())

    def __repr__(self) -> str:
        args = ", ".join(f"{k}={v}" for k, v in self.display_dict().items())
        return f"{self._modelname}({args})"

    def __str__(self) -> str:
        d = self.display_dict()
        return yaml.dump(d, sort_keys=False, indent=4)  # type: ignore

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, BalsamModel):
            return False
        return self._state == "clean" and other._state == "clean" and self._read_model == other._read_model


class NonCreatableBalsamModel(BalsamModel):
    id: Field[int]


class CreatableBalsamModel(BalsamModel):
    id: Field[Optional[int]]
