from rest_framework import serializers
from django.core.exceptions import ObjectDoesNotExist


class CachedPrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    def to_internal_value(self, data):
        """
        Cache fetched objects by PK on the request context
        """
        if self.pk_field is not None:
            data = self.pk_field.to_internal_value(data)

        if not isinstance(data, int):
            raise serializers.ValidationError(
                f"{self.field_name} must be an integer ID; not {type(data).__name__}"
            )

        cache = self.context.setdefault(f"{self.field_name}_cache", {})
        if data in cache:
            return cache[data]
        try:
            cache[data] = self.get_queryset().get(pk=data)
            return cache[data]
        except ObjectDoesNotExist:
            self.fail("does_not_exist", pk_value=data)
        except (TypeError, ValueError):
            self.fail("incorrect_type", data_type=type(data).__name__)


class BulkListSerializer(serializers.ListSerializer):
    def __init__(self, *args, **kwargs):
        self.pk_patches = kwargs.pop("pk_patches", False)
        super().__init__(*args, **kwargs)

        # If pk_patches=True, we are doing a bulk PATCH and need
        # to accept pk as a writeable field.
        if self.pk_patches:
            self.child.fields["pk"] = serializers.IntegerField(required=True)

            # If there are any nested serializers being updated,
            # we need to make their pk's writeable, too.
            names = getattr(self.child.Meta, "nested_update_fields", [])
            for name in names:
                nested_serializer = self.child.fields[name]
                if isinstance(nested_serializer, serializers.ListSerializer):
                    child = nested_serializer.child
                else:
                    child = nested_serializer
                child.fields["pk"] = serializers.IntegerField(required=True)

        if getattr(self, "initial_data", None):
            cache_fields = [
                f
                for f in self.child.fields
                if isinstance(f, CachedPrimaryKeyRelatedField)
            ]
            for field in cache_fields:
                self._cache_related_field_by_pk(field)

    def _cache_related_field_by_pk(self, field):
        name = field.field_name
        pk_values = set(dat[name] for dat in self.initial_data if name in dat)
        cache = {obj.pk: obj for obj in field.get_queryset().filter(pk__in=pk_values)}
        self.context[f"{name}_cache"] = cache

    def create(self, validated_data):
        ModelClass = self.child.Meta.model
        instances = ModelClass.objects.bulk_create(validated_data)
        return instances

    def update(self, instance, validated_data):
        if self.pk_patches:
            return self.update_pk_patches(instance, validated_data)
        return self.update_queryset(instance, validated_data)

    def update_pk_patches(self, instance, validated_data):
        """
        Bulk partial-update: no creation/deletion/reordering
        """
        allowed_pks = instance.values_list("pk", flat=True)
        patch_list = []
        for patch in validated_data:
            pk = patch["pk"]
            if pk in allowed_pks:
                patch_list.append(patch)
            else:
                raise serializers.ValidationError(f"Invalid pk: {pk}")
        ModelClass = self.child.Meta.model
        res = ModelClass.objects.bulk_update(patch_list)
        return res

    def update_queryset(self, instance, validated_data):
        if "pk" in validated_data:
            raise serializers.ValidationError(
                "Do not provide `pk`: bulk update applies to entire query"
            )
        patch_list = list(instance.values("pk"))
        for patch in patch_list:
            patch.update(validated_data[0])
        ModelClass = self.child.Meta.model
        res = ModelClass.objects.bulk_update(patch_list)
        return res


class BulkModelSerializer(serializers.ModelSerializer):
    @classmethod
    def many_init(cls, *args, **kwargs):
        """
        This method implements the creation of a `ListSerializer` parent
        class when `many=True` is used. Customized to ensure that
        "pk_patches" kwarg is only passed to parent, and not the child
        """
        allow_empty = kwargs.pop("allow_empty", None)
        pk_patches = kwargs.pop("pk_patches", False)
        child_serializer = cls(*args, **kwargs)
        list_kwargs = {"child": child_serializer, "pk_patches": pk_patches}
        if allow_empty is not None:
            list_kwargs["allow_empty"] = allow_empty
        list_kwargs.update(
            {
                key: value
                for key, value in kwargs.items()
                if key in serializers.LIST_SERIALIZER_KWARGS
            }
        )
        return BulkListSerializer(*args, **list_kwargs)
