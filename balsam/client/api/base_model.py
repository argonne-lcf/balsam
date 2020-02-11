# from balsam.client import query
from uuid import UUID
from pydantic import BaseModel
from typing import Union


class PydanticModel(BaseModel):
    class Config:
        extra = "forbid"
        validate_assignment = True

    pk: Union[None, int, UUID] = None


class BalsamModel(object):
    def __init__(self, **kwargs):
        """
        Populate instance Fields from kwargs
        """
        self._pydantic_data = self.__class__.DataClass(**kwargs)

    def __setattr__(self, name, value):
        if name in self.DataClass.__fields__:
            setattr(self._pydantic_data, name, value)
        else:
            super().__setattr__(name, value)

    def __getattr__(self, name):
        print("Trying to get:", name)
        if name in self.DataClass.__fields__:
            return getattr(self._pydantic_data, name)
        else:
            raise AttributeError(f"No such attribute {name}")

    def __repr__(self):
        values = ", ".join(
            f"{k}={repr(v)}" for k, v in self._pydantic_data.__values__.items()
        )
        return f"{self.__class__.__name__}({values})"


#    @property
#    def pk(self):
#        return self._fields['pk']
#
#    def save(self):
#        if self.pk is not None:
#            self._do_update()
#        else:
#            self.objects.create([self])
#        self._modified_fields.clear()
#
#    def _do_update(self):
#        if not self._modified_fields:
#            return
#        update_dict = {k: self._fields[k] for k in self._modified_fields}
#        self.objects.filter(pk=self.pk).update(**update_dict)
#
#    def refresh_from_db(self):
#        if self.pk is None:
#            raise AttributeError(f'Cannot refresh instance without PK')
#        from_db = self.objects.get(pk=self.pk)
#        self._fields = from_db._fields.copy()
#        self._modified_fields.clear()
#
#    def delete(self):
#        if self.pk is None:
#            raise AttributeError(f'Cannot delete instance without PK')
#        self.objects.filter(pk=self.pk).delete()
#        self._fields['pk'] = None
#
#    class DoesNotExist(Exception):
#        pass
#
#    class MultipleObjectsReturned(Exception):
#        def __init__(self, nobj):
#            super().__init__(f'Returned {nobj} objects; expected one!')
