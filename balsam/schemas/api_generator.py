import subprocess
import sys
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type

import click
import jinja2
from pydantic import BaseModel, PyObject
from pydantic.fields import ModelField

from balsam.api.model_base import BalsamModel

FieldDict = Dict[str, Any]

env = jinja2.Environment(trim_blocks=True, lstrip_blocks=True)
master_template = env.from_string(
    """
# This file was auto-generated via {{ generator_name }}
# [git rev {{git_ref}}]
# Do *not* make changes to the API by changing this file!

{% for imp_statement in import_modules -%}
{{ imp_statement }}
{% endfor %}

class {{model_name}}({{model_base}}):
    create_model_cls = {{create_model_cls}}
    update_model_cls = {{update_model_cls}}
    read_model_cls = {{read_model_cls}}
    objects: "{{manager_name}}"

    {% for field in model_fields.values() %}
    {% if field.allowed_none %}
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
        _kwargs = {k: v for k,v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)
    {% endif %}

class {{query_name}}(Query[{{model_name}}]):
    def get(
        self,
        {% for line in model_filter_kwargs %}
        {{line}},
        {% endfor %}
    ) -> {{model_name}}:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        {% for line in model_filter_kwargs %}
        {{line}},
        {% endfor %}
    ) -> "{{query_name}}":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    {% if model_update_kwargs %}
    def update(
        self,
        {% for line in model_update_kwargs %}
        {{line}},
        {% endfor %}
    ) -> List[{{model_name}}]:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)
    {% endif %}

    {% if order_by_type %}
    def order_by(self, field: Optional[{{order_by_type}}]) -> "{{query_name}}":
        return self._order_by(field)
    {% endif %}

class {{manager_name}}(Manager[{{model_name}}], {{manager_mixin}}):
    path = "{{manager_url}}"
    model_class = {{model_name}}
    query_class = {{query_name}}

    {% if model_create_kwargs %}
    def create(
        self,
        {% for line in model_create_kwargs %}
        {{line}},
        {% endfor %}
    ) -> {{model_name}}:
        kwargs = {k: v for k,v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**_kwargs)
    {% endif %}

    def get(
        self,
        {% for line in model_filter_kwargs %}
        {{line}},
        {% endfor %}
    ) -> {{model_name}}:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return {{query_name}}(manager=self).get(**kwargs)

    def filter(
        self,
        {% for line in model_filter_kwargs %}
        {{line}},
        {% endfor %}
    ) -> "{{query_name}}":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return {{query_name}}(manager=self).filter(**kwargs)

"""
)


def get_git_hash() -> str:
    return subprocess.check_output("git rev-parse --short HEAD", shell=True, encoding="utf-8").strip()


def field_to_dict(field: ModelField, schema: BaseModel) -> FieldDict:
    if field.default == [] or field.default == {}:
        assert not field.required
        default_create = None
    elif not field.required:
        default_create = field.default
    else:
        assert field.default in [..., None], f"Expected {field} of {schema} to have default=Ellipsis or None"
        assert field.required
        default_create = ...
    if field.is_complex():
        annotation = None
        for cls in schema.mro():  # type: ignore
            if field.name in cls.__annotations__:
                annotation = cls.__annotations__[field.name]
                break
        else:
            raise RuntimeError(f"Could not find annotation for {field.name} on {schema.__name__} MRO")  # type: ignore
    else:
        annotation = field.type_.__name__

    assert annotation is not None
    return {
        "name": field.name,
        "required": field.required,
        "annotation": annotation,
        "schema_default": field.default,  # default attribute on the Pydantic ModelField
        "default_create": default_create,  # default value for __init__ kwargs
        "optional_create": default_create is None,  # whether to use Optional[] annotation in __init__
    }


def get_schema_fields(schema: BaseModel) -> Dict[str, FieldDict]:
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
            result += [f'{field["name"]}: Optional[{field["annotation"]}] = {field["default_create"]}']
        else:
            result += [f'{field["name"]}: {field["annotation"]} = {field["default_create"]}']
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
        kwarg = f"{field.name}: Optional[{field_type}] = None"
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
    create_model = model_base.__dict__["create_model_cls"]
    update_model = model_base.__dict__["update_model_cls"]
    read_model = model_base.__dict__["read_model_cls"]

    create_fields = get_schema_fields(create_model) if create_model is not None else {}
    update_fields = get_schema_fields(update_model) if update_model is not None else {}
    read_fields = get_schema_fields(read_model)
    return create_fields, update_fields, read_fields


def qual_path(obj: type) -> str:
    return f"{obj.__module__}.{obj.__name__}"


def get_model_ctx(model_base: Type[BalsamModel], manager_mixin: type, filterset: type) -> Dict[str, Any]:
    base_name = model_base.__name__
    name = base_name[: base_name.find("Base")]
    manager_name = f"{name}Manager"
    query_name = f"{name}Query"

    create_fields, update_fields, read_fields = get_model_fields(model_base)
    create_kwargs = model_create_signature(create_fields) if create_fields else None
    update_kwargs = update_signature(update_fields) if update_fields else None
    filter_kwargs = filter_signature(filterset)
    order_by_type = order_by_typename(filterset)
    fields = {**create_fields, **update_fields, **read_fields}
    for field in fields:
        # A read-only field can be None (e.g. not created yet, id is None)
        if field in read_fields and field not in create_fields:
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
        create_model_cls=qual_path(model_base.__dict__["create_model_cls"]),
        update_model_cls=qual_path(model_base.__dict__["update_model_cls"]),
        read_model_cls=qual_path(model_base.__dict__["read_model_cls"]),
        manager_name=manager_name,
        query_name=query_name,
        model_fields=fields,
        model_create_kwargs=create_kwargs,
        manager_mixin=qual_path(manager_mixin),
        manager_url=manager_mixin.path,  # type: ignore
        model_update_kwargs=update_kwargs,
        model_filter_kwargs=filter_kwargs,
        order_by_type=order_by_type,
    )


class APIConf(BaseModel):
    model_base: PyObject
    manager_mixin: PyObject
    filterset: PyObject


@click.command()
@click.argument("model-base")
@click.argument("manager-mixin")
@click.argument("filterset")
def main(model_base: str, manager_mixin: str, filterset: str) -> None:
    conf = APIConf(model_base=model_base, manager_mixin=manager_mixin, filterset=filterset)
    model_ctx = get_model_ctx(conf.model_base, conf.manager_mixin, conf.filterset)  # type: ignore

    imports = [
        "import balsam.api.model_base",
        "import balsam.api.manager_base",
        "import balsam.api.models.bases",
        "import balsam.server.routers.filters",
        "from balsam.api.model_base import Field",
    ]

    result = master_template.render(
        generator_name=f"{sys.executable} {' '.join(sys.argv)}",
        git_ref=get_git_hash(),
        import_modules=imports,
        **model_ctx,
    )
    print(result)


if __name__ == "__main__":
    main()
