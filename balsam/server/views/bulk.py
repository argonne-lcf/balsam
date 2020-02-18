from rest_framework.response import Response
from rest_framework import status
from rest_framework.serializers import ValidationError
from rest_framework.mixins import (
    ListModelMixin,
    CreateModelMixin,
    UpdateModelMixin,
    DestroyModelMixin,
)
from rest_framework.generics import GenericAPIView


class BulkCreateModelMixin(CreateModelMixin):
    """
    Create a list of model instances.
    """

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, many=True)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)
        return Response(
            serializer.data, status=status.HTTP_201_CREATED, headers=headers
        )


class BulkUpdateModelMixin(UpdateModelMixin):
    """
    Update model instances.
    """

    def update(self, request, *args, **kwargs):
        qs = self.filter_queryset(self.get_queryset())
        pk_patches = kwargs.get("pk_patches", False)
        if not pk_patches:
            if not isinstance(request.data, dict):
                raise ValidationError(
                    "Bulk PUT does not accept list data: provide a dict"
                )
            data = [request.data]
        else:
            data = request.data

        serializer = self.get_serializer(
            qs, data=data, many=True, partial=True, pk_patches=kwargs.get("pk_patches"),
        )
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)

        if getattr(qs, "_prefetched_objects_cache", None):
            # If 'prefetch_related' has been applied to a queryset, we need to
            # forcibly invalidate the prefetch cache on the instance.
            qs._prefetched_objects_cache = {}

        return Response(serializer.data)

    def partial_update(self, request, *args, **kwargs):
        return self.update(request, *args, **kwargs)


class BulkDestroyModelMixin(DestroyModelMixin):
    """
    Destroy model instances.
    """

    def destroy(self, request, *args, **kwargs):
        full_count = self.get_queryset().count()
        qs = self.filter_queryset(self.get_queryset())
        filtered_count = qs.count()
        destroy_ok = request.query_params.get("destroy_all") == "yes"

        if filtered_count < full_count or destroy_ok:
            self.perform_destroy(qs)
            return Response(status=status.HTTP_204_NO_CONTENT)

        msg = (
            "It's dangerous to delete all items. "
            "Either provide a filter to delete a subset of items, "
            "or pass the query parameter destroy_all=yes"
        )
        return Response({"errors": msg}, status=status.HTTP_400_BAD_REQUEST)

    def perform_destroy(self, queryset):
        raise NotImplementedError(f"Override perform_destroy to enable bulk destroy.")


class ListBulkCreateAPIView(ListModelMixin, BulkCreateModelMixin, GenericAPIView):
    """
    Concrete view for listing a queryset or bulk-creating a model instance.
    """

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.create(request, *args, **kwargs)


class ListBulkUpdateAPIView(ListModelMixin, BulkUpdateModelMixin, GenericAPIView):
    """
    Concrete view for listing a queryset or bulk-updating a model instance.
    """

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        return self.update(request, *args, pk_patches=False, **kwargs)

    def patch(self, request, *args, **kwargs):
        return self.update(request, *args, pk_patches=True, **kwargs)


class ListBulkCreateBulkUpdateAPIView(
    ListModelMixin, BulkCreateModelMixin, BulkUpdateModelMixin, GenericAPIView
):
    """
    Concrete view for listing, bulk-creating, or bulk-updating
    """

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.create(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        return self.update(request, *args, pk_patches=False, **kwargs)

    def patch(self, request, *args, **kwargs):
        return self.update(request, *args, pk_patches=True, **kwargs)


class ListBulkCreateBulkUpdateBulkDestroyAPIView(
    ListModelMixin,
    BulkCreateModelMixin,
    BulkUpdateModelMixin,
    BulkDestroyModelMixin,
    GenericAPIView,
):
    """
    Concrete view for listing, bulk-creating, bulk-updating, or bulk-destroying
    """

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.create(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        return self.update(request, *args, pk_patches=False, **kwargs)

    def patch(self, request, *args, **kwargs):
        return self.update(request, *args, pk_patches=True, **kwargs)

    def delete(self, request, *args, **kwargs):
        return self.destroy(request, *args, **kwargs)


class ListSingleCreateBulkUpdateAPIView(
    ListModelMixin, CreateModelMixin, BulkUpdateModelMixin, GenericAPIView
):
    """
    Concrete view for listing, creating single instances, or bulk-updating
    """

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.create(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        return self.update(request, *args, pk_patches=False, **kwargs)

    def patch(self, request, *args, **kwargs):
        return self.update(request, *args, pk_patches=True, **kwargs)
