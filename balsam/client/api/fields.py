class Field:
    """
    Field Descriptor classes represent each type of Model attributes
    """
    def __init__(self, default=None, required=True, validators=[]):
        self.name = None
        self.default = default
        if required and self.default is not None:
            raise ValueError(f"Required Field {self.name} cannot have a default value.")
        self.required = required
        self.validators = validators
        assert all(callable(v) for v in validators)

    def __get__(self, instance, instance_type):
        if instance is None: return self
        return instance._fields.get(self.name, self.default)

    def __set__(self, instance, value):
        for validate in self.validators: 
            value = validate(value)
        instance._fields[self.name] = value
        instance._modified_fields.add(self.name) = True
