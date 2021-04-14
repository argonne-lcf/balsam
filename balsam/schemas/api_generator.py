import subprocess
import sys
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type

import jinja2
from pydantic import BaseModel, PyObject
from pydantic.fields import ModelField

from balsam._api.model import BalsamModel

FieldDict = Dict[str, Any]

env = jinja2.Environment(trim_blocks=True, lstrip_blocks=True)
header_template = env.from_string(
    """
# This file was auto-generated via {{ generator_name }}
# [git rev {{git_ref}}]
# Do *not* make changes to the API by changing this file!

{% for imp_statement in import_modules -%}
{{ imp_statement }}
{% endfor %}

    """
)
master_template = env.from_string(
    """
class {{model_name}}({{model_base}}):
    _create_model_cls = {{_create_model_cls}}
    _update_model_cls = {{_update_model_cls}}
    _read_model_cls = {{_read_model_cls}}
    objects: "{{manager_name}}"

    {% for field in model_fields.values() %}
    {% if field.allowed_none and 'Optional' not in field.annotation %}
    {{field.name}} = Field[Optional[{{field.annotation}}]]()
    {% else %}
    {{field.name}} = Field[{{field.annotation}}]()
    {% endif %}
    {% endfor %}

    {% if model_create_kwargs %}
    def __init__(
        self,
        {% for line in model_create_kwargs %}
        {{line}},
        {% endfor %}
        **kwargs: Any,
    ) -> None:
        '''
        Construct a new {{model_name}} object.  You must eventually call the save() method or
        pass a {{model_name}} list into {{model_name}}.objects.bulk_create().

        {% for line in model_create_help %}
        {{line}}
        {% endfor %}
        '''
        _kwargs = {k: v for k,v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)
    {% endif %}

class {{query_name}}(Query[{{model_name}}]):
    {% if not model_filter_kwargs and not order_by_type %}
    pass
    {% endif %}
    {% if model_filter_kwargs %}
    def get(
        self,
        {% for line in model_filter_kwargs %}
        {{line}},
        {% endfor %}
    ) -> {{model_name}}:
        '''
        Retrieve exactly one {{model_name}}. Raises {{model_name}}.DoesNotExist
        if no items were found, or {{model_name}}.MultipleObjectsReturned if
        more than one item matched the query.

        {% for line in model_filter_help %}
        {{line}}
        {% endfor %}
        '''
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        {% for line in model_filter_kwargs %}
        {{line}},
        {% endfor %}
    ) -> "{{query_name}}":
        '''
        Retrieve exactly one {{model_name}}. Raises {{model_name}}.DoesNotExist
        if no items were found, or {{model_name}}.MultipleObjectsReturned if
        more than one item matched the query.

        {% for line in model_filter_help %}
        {{line}}
        {% endfor %}
        '''
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)
    {% endif %}

    {% if model_update_kwargs %}
    def update(
        self,
        {% for line in model_update_kwargs %}
        {{line}},
        {% endfor %}
    ) -> List[{{model_name}}]:
        '''
        Updates all items selected by this query with the given values.

        {% for line in model_update_help %}
        {{line}}
        {% endfor %}
        '''
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)
    {% endif %}

    {% if order_by_type %}
    def order_by(self, field: Optional[{{order_by_type}}]) -> "{{query_name}}":
        '''
        Order the returned items by this field.
        '''
        return self._order_by(field)
    {% endif %}

class {{manager_name}}({{manager_base}}):
    _api_path = "{{manager_url}}"
    _model_class = {{model_name}}
    _query_class = {{query_name}}
    _bulk_create_enabled = {{_bulk_create_enabled}}
    _bulk_update_enabled = {{_bulk_update_enabled}}
    _bulk_delete_enabled = {{_bulk_delete_enabled}}
    _paginated_list_response = {{_paginated_list_response}}

    {% if model_create_kwargs %}
    def create(
        self,
        {% for line in model_create_kwargs %}
        {{line}},
        {% endfor %}
    ) -> {{model_name}}:
        '''
        Create a new {{model_name}} object and save it to the API in one step.

        {% for line in model_create_help %}
        {{line}}
        {% endfor %}
        '''
        kwargs = {k: v for k,v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)
    {% endif %}

    def all(self) -> "{{query_name}}":
        '''
        Returns a Query for all {{model_name}} items.
        '''
        return self._query_class(manager=self)

    {% if model_filter_kwargs %}
    def get(
        self,
        {% for line in model_filter_kwargs %}
        {{line}},
        {% endfor %}
    ) -> {{model_name}}:
        '''
        Retrieve exactly one {{model_name}}. Raises {{model_name}}.DoesNotExist
        if no items were found, or {{model_name}}.MultipleObjectsReturned if
        more than one item matched the query.

        {% for line in model_filter_help %}
        {{line}}
        {% endfor %}
        '''
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return {{query_name}}(manager=self).get(**kwargs)

    def filter(
        self,
        {% for line in model_filter_kwargs %}
        {{line}},
        {% endfor %}
    ) -> "{{query_name}}":
        '''
        Returns a {{model_name}} Query returning items matching the filter criteria.

        {% for line in model_filter_help %}
        {{line}}
        {% endfor %}
        '''
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return {{query_name}}(manager=self).filter(**kwargs)
    {% endif %}

"""
)


def get_git_hash() -> str:
    return subprocess.check_output("git rev-parse --short HEAD", shell=True, encoding="utf-8").strip()


def field_to_dict(field: ModelField, schema: Type[BaseModel]) -> FieldDict:
    field_default = field.field_info.default
    if field_default == [] or field_default == {}:
        assert not field.required
        default_create = None
    elif not field.required:
        default_create = field_default
    else:
        assert field_default == ..., f"Expected required field {field} of {schema} to have default=Ellipsis"
        assert field.required
        default_create = ...
    if field.is_complex():
        annotation = None
        for cls in schema.mro():
            if field.name in cls.__annotations__:
                annotation = str(cls.__annotations__[field.name])
                break
        else:
            raise RuntimeError(f"Could not find annotation for {field.name} on {schema.__name__} MRO")
    else:
        annotation = qual_path(field.type_)

    assert annotation is not None
    annotation = annotation.replace("NoneType", "None")
    return {
        "name": field.name,
        "required": field.required,
        "description": getattr(field.field_info, "description", ""),
        "annotation": annotation,
        "schema_default": field_default,  # default attribute on the Pydantic ModelField
        "default_create": default_create,  # default value for __init__ kwargs
        "optional_create": default_create is None,  # whether to use Optional[] annotation in __init__
    }


def get_schema_fields(schema: Type[BaseModel]) -> Dict[str, FieldDict]:
    return {k: field_to_dict(v, schema) for k, v in schema.__fields__.items()}


def model_create_signature(create_fields: FieldDict) -> List[str]:
    fields = create_fields.copy()
    required_fields = dict((k, v) for k, v in fields.items() if v["required"])
    for k in required_fields:
        del fields[k]

    result = []
    for field in required_fields.values():
        result += [f'{field["name"]}: {field["annotation"]}']
    for field in fields.values():
        if field["optional_create"]:
            result += [f'{field["name"]}: Optional[{field["annotation"]}] = {repr(field["default_create"])}']
        else:
            result += [f'{field["name"]}: {field["annotation"]} = {repr(field["default_create"])}']
    return result


def filter_signature(filterset: object) -> List[str]:
    result = []
    for field in filterset.__dataclass_fields__.values():  # type: ignore
        if field.name == "ordering":
            continue
        try:
            field_type = qual_path(field.type)
        except AttributeError:
            field_type = str(field.type)
        if getattr(field.type, "_name", None) in ["List", "Set"]:
            inner_type = qual_path(field.type.__args__[0])
            annotation = f"Union[{field_type}, {inner_type}, None]"
        else:
            annotation = f"Optional[{field_type}]"
        kwarg = f"{field.name}: {annotation} = None"
        result.append(kwarg)
    return result


def update_signature(update_fields: FieldDict) -> List[str]:
    result = []
    for field in update_fields.values():
        result += [f'{field["name"]}: Optional[{field["annotation"]}] = None']
    return result


def order_by_typename(filterset: object) -> Optional[str]:
    if "ordering" not in filterset.__dataclass_fields__:  # type: ignore
        return None
    order_enum = filterset.__dataclass_fields__["ordering"].type  # type: ignore
    assert issubclass(order_enum, Enum)
    typename = qual_path(order_enum)
    return typename
    return f"order_param: Optional[{typename}] = None"


def get_model_fields(model_base: Type[BalsamModel]) -> Tuple[FieldDict, FieldDict, FieldDict]:
    create_model = model_base._create_model_cls
    update_model = model_base._update_model_cls
    read_model = model_base._read_model_cls

    create_fields = get_schema_fields(create_model) if create_model is not None else {}
    update_fields = get_schema_fields(update_model) if update_model is not None else {}
    read_fields = get_schema_fields(read_model)
    return create_fields, update_fields, read_fields


def qual_path(obj: type) -> str:
    if obj is None:
        return "None"
    mod, name = str(obj.__module__), str(obj.__name__)
    if mod == "builtins":
        return name
    return f"{mod}.{name}"


def make_help_text(fields: FieldDict) -> List[str]:
    result: List[str] = []
    if not fields:
        return result
    maxwidth = max(len(f) for f in fields.keys()) + 1
    for field_name, field in fields.items():
        if field_name == "ordering":
            continue
        try:
            descr = field["description"]
        except Exception:
            descr = field.default.description
        result.append(f"{(field_name+':').ljust(maxwidth)} {descr}")
    return result


def get_model_ctx(model_base: Type[BalsamModel], manager_base: type, filterset: type) -> Dict[str, Any]:
    base_name = model_base.__name__
    name = base_name[: base_name.find("Base")]
    manager_name = f"{name}Manager"
    query_name = f"{name}Query"
    base_name = qual_path(model_base)

    create_fields, update_fields, read_fields = get_model_fields(model_base)
    create_kwargs = model_create_signature(create_fields) if create_fields else None
    update_kwargs = update_signature(update_fields) if update_fields else None
    filter_kwargs = filter_signature(filterset)
    order_by_type = order_by_typename(filterset)

    model_create_help = make_help_text(create_fields)
    model_update_help = make_help_text(update_fields)
    model_filter_help = make_help_text(filterset.__dataclass_fields__)  # type: ignore
    fields = {**create_fields, **update_fields, **read_fields}
    for field in fields:
        # A read-only field can be None if the model is creatable (e.g. not created yet, id is None)
        if field in read_fields and field not in create_fields and model_base._create_model_cls is not None:
            fields[field]["allowed_none"] = True
        # A write-only field can be None (e.g. state_timestamp not written yet)
        elif field in update_fields and field not in read_fields:
            fields[field]["allowed_none"] = True
        # A field that can actually be null in the API:
        elif field in read_fields and read_fields[field]["schema_default"] is None:
            fields[field]["allowed_none"] = True
        else:
            fields[field]["allowed_none"] = False

    return dict(
        model_name=name,
        model_base=base_name,
        _create_model_cls=qual_path(model_base.__dict__["_create_model_cls"]),
        _update_model_cls=qual_path(model_base.__dict__["_update_model_cls"]),
        _read_model_cls=qual_path(model_base.__dict__["_read_model_cls"]),
        manager_name=manager_name,
        query_name=query_name,
        model_fields=fields,
        model_create_kwargs=create_kwargs,
        manager_base=qual_path(manager_base),
        manager_url=manager_base._api_path,  # type: ignore
        _bulk_create_enabled=getattr(manager_base, "_bulk_create_enabled", False),
        _bulk_update_enabled=getattr(manager_base, "_bulk_update_enabled", False),
        _bulk_delete_enabled=getattr(manager_base, "_bulk_delete_enabled", False),
        _paginated_list_response=getattr(manager_base, "_paginated_list_response", True),
        model_update_kwargs=update_kwargs,
        model_filter_kwargs=filter_kwargs,
        order_by_type=order_by_type,
        model_create_help=model_create_help,
        model_filter_help=model_filter_help,
        model_update_help=model_update_help,
    )


class APIConf(BaseModel):
    model_base: PyObject
    manager_base: PyObject
    filterset: PyObject


def main() -> None:
    imports = [
        "import datetime",
        "import typing",
        "from typing import Optional, Any, List, Union",
        "import pathlib",
        "import uuid",
        "import pydantic",
        "import balsam._api.model",
        "import balsam._api.bases",
        "from balsam._api.query import Query",
        "from balsam._api.model import Field",
    ]
    header = header_template.render(
        generator_name=f"{sys.executable} {' '.join(sys.argv)}",
        git_ref=get_git_hash(),
        import_modules=imports,
    )
    print(header)

    models = [
        "Site",
        "App",
        "Job",
        "BatchJob",
        "Session",
        "TransferItem",
        "EventLog",
    ]
    for model in models:
        conf = APIConf(
            model_base=f"balsam._api.bases.{model}Base",
            manager_base=f"balsam._api.bases.{model}ManagerBase",
            filterset=f"balsam.server.routers.filters.{model}Query",
        )
        model_ctx = get_model_ctx(conf.model_base, conf.manager_base, conf.filterset)  # type: ignore
        result = master_template.render(**model_ctx)
        print(result)


if __name__ == "__main__":
    main()
