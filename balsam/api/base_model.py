from pydantic import BaseModel


class BalsamModel(BaseModel):
    class Config:
        extra = "forbid"
        validate_assignment = True

    def save(self):
        print(
            f"Saving {self.__class__.__name__} {self.pk} via {self.__class__.objects}"
        )
        if self.pk is not None:
            self.__class__.objects._do_update(self)
        else:
            self.objects.create([self])

    def refresh_from_db(self):
        if self.pk is None:
            raise AttributeError(f"Cannot refresh instance without PK")
        from_db = self.objects.get(pk=self.pk)
        self._fields = from_db._fields.copy()

    def delete(self):
        if self.pk is None:
            raise AttributeError(f"Cannot delete instance without PK")
        self.objects.filter(pk=self.pk).delete()
        self._fields["pk"] = None

    class DoesNotExist(Exception):
        pass

    class MultipleObjectsReturned(Exception):
        def __init__(self, nobj):
            super().__init__(f"Returned {nobj} objects; expected one!")


# class BalsamModel(object):
#     def __init__(self, **kwargs):
#         """
#         Populate instance Fields from kwargs
#         """
#         self._pydantic_data = self.__class__.DataClass(**kwargs)
#
#     def __setattr__(self, name, value):
#         if name in self.DataClass.__fields__:
#             setattr(self._pydantic_data, name, value)
#         else:
#             super().__setattr__(name, value)
#
#     def __getattr__(self, name):
#         print("Trying to get:", name)
#         if name in self.DataClass.__fields__:
#             return getattr(self._pydantic_data, name)
#         else:
#             raise AttributeError(f"No such attribute {name}")
#
#     def __repr__(self):
#         values = ", ".join(
#             f"{k}={repr(v)}" for k, v in self._pydantic_data.__values__.items()
#         )
#         return f"{self.__class__.__name__}({values})"
