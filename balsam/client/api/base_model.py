from .fields import Field
from balsam.client import query

class ModelMeta(type):
    def __new__(meta, name, bases, class_dict):
        required_fields = set()
        field_names = set()

        for name, obj in class_dict.items():
            if isinstance(obj, Field):
                obj.name = name
                field_names.add(name)
                if obj.required: required_fields.add(name)

        class_dict['_field_names'] = field_names
        class_dict['_required_fields'] = required_fields
        ModelClass = super().__new__(meta, name, bases, class_dict)
        if 'objects' not in class_dict:
            ModelClass.objects = query.Query(ModelClass)
        else:
            assert isinstance(ModelClass.objects, query.Query)
            ModelClass.objects.model_class = ModelClass
        return ModelClass


class Model(object, metaclass=ModelMeta):
    def __init__(self, **kwargs):
        """
        Populate instance Fields from kwargs
        """
        self._modified_fields = set()
        self._fields = {
            'pk': kwargs.pop('pk')
        }

        for field in self._required_fields:
            try:
                value = kwargs.pop(field)
            except KeyError:
                raise ValueError(
                    f'{self.__class__.__name__} missing required Field '
                    f'{field} (required_fields: {self._required_fields})'
                )
            else:
                setattr(self, field, value)

        for field, value in kwargs.items():
            if field not in self._field_names:
                raise ValueError(f'Invalid kwarg: no Field called {field}')
            setattr(self, field, value)

    @property
    def pk(self):
        return self._fields['pk']

    def save(self):
        if self.pk is not None:
            self._do_update()
        else:
            self.objects.create([self])
        self._modified_fields.clear()

    def _do_update(self):
        if not self._modified_fields:
            return
        update_dict = {k: self._fields[k] for k in self._modified_fields}
        self.objects.filter(pk=self.pk).update(**update_dict)

    def refresh_from_db(self):
        if self.pk is None:
            raise AttributeError(f'Cannot refresh instance without PK')
        from_db = self.objects.get(pk=self.pk)
        self._fields = from_db._fields.copy()
        self._modified_fields.clear()

    def delete(self):
        if self.pk is None:
            raise AttributeError(f'Cannot delete instance without PK')
        self.objects.filter(pk=self.pk).delete()
        self._fields['pk'] = None

    class DoesNotExist(Exception):
        pass

    class MultipleObjectsReturned(Exception):
        def __init__(self, nobj):
            super().__init__(f'Returned {nobj} objects; expected one!')
