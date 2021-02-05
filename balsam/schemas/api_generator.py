from balsam.api.model_base import BalsamModel
from balsam.api.models.bases import SiteBase, SiteManagerMixin
from balsam.server.routers.filters import SiteQuery


def field_to_dict(field, schema):
    if field.default == [] or field.default == {}:
        assert not field.required
        default = None
    elif not field.required:
        default = field.default
    else:
        assert field.default is ...
        assert field.required
        default = ...
    if field.is_complex():
        annotation = schema.__annotations__[field.name]
    else:
        annotation = field.type_.__name__
    return {
        "name": field.name,
        "required": field.required,
        "annotation": annotation,
        "default": default,
        "optional_type": field.default is None,
    }


def model_create_signature(schema):
    fields = {k: field_to_dict(v, schema) for k, v in schema.__fields__.items()}
    required_fields = dict((k, v) for k, v in fields.items() if v["required"])
    for k in required_fields:
        del fields[k]

    result = []
    for field in required_fields.values():
        result += [f'{field["name"]}: {field["annotation"]},\n']
    for field in fields.values():
        if field["optional_type"]:
            result += [f'{field["name"]}: Optional[{field["annotation"]}] = {field["default"]},\n']
        else:
            result += [f'{field["name"]}: {field["annotation"]} = {field["default"]},\n']
    return result


def model_init(schema):
    sig = model_create_signature(schema)
    result = 4 * " " + "def __init__(\n"
    result += 8 * " " + "self,\n"
    for line in sig:
        result += 8 * " " + line
    result += 8 * " " + "**kwargs: Any\n"
    result += 4 * " " + ") -> None:\n"
    result += (
        8 * " "
        + '_kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}\n'
    )
    result += 8 * " " + "_kwargs.update(kwargs)\n"
    result += 8 * " " + "return super().__init__(**_kwargs)\n\n"
    return result


def render_model(model_base: BalsamModel) -> str:
    base_name = model_base.__name__
    name = base_name[: base_name.find("Base")]
    manager_name = f"{name}Manager"
    print(manager_name)
    print(model_init(model_base.__dict__["create_model_cls"]))

    field_names = set()
    for model_cls in [
        model_base.__dict__["create_model_cls"],
        model_base.__dict__["update_model_cls"],
        model_base.__dict__["read_model_cls"],
    ]:
        if model_cls is not None:
            field_names.update(model_cls.__fields__)


def generate(model_base: BalsamModel, manager_mixin: object, filterset: object) -> str:
    model = render_model(model_base)
    # manager = render_manager(model_base, manager_base, filterset)
    # query = render_query(model_base, filterset)
    print(model)


if __name__ == "__main__":
    print(generate(SiteBase, SiteManagerMixin, SiteQuery))
