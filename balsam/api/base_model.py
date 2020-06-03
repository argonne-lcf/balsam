from pydantic import BaseModel


class BalsamModel(BaseModel):
    class Config:
        extra = "ignore"
        validate_assignment = True

    def save(self):
        if self.pk is not None:
            self.__class__.objects._do_update(self)
        else:
            created = self.__class__.objects.create(**self.dict())
            for k in created.__fields__:
                setattr(self, k, getattr(created, k))

    def refresh_from_db(self):
        if self.pk is None:
            raise AttributeError(f"Cannot refresh instance without PK")
        from_db = self.__class__.objects.get(pk=self.pk)
        for k in from_db.__fields__:
            setattr(self, k, getattr(from_db, k))

    def delete(self):
        if self.pk is None:
            raise AttributeError(f"Cannot delete instance without PK")
        self.__class__.objects._do_delete(self)
        self.pk = None

    class DoesNotExist(Exception):
        pass

    class MultipleObjectsReturned(Exception):
        def __init__(self, nobj):
            super().__init__(f"Returned {nobj} objects; expected one!")
