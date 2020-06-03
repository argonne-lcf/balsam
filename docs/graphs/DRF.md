Disable caching on GET views that need to stay up-to-date
Use @never_cache decorator

Avoid chatty APIs: bulk create, update, acquire
Avoid extraneous fetching: pagination and nesting URIs

API versioning:
    put entire application under /api/v1/ namespace
    Then, if Schema has to change, can start serving /api/v2/ in parallel

Tracing a DRF bulk create
--------------------------
In Create view auth:
    SELECT django_session
    SELECT balsam_user

Serializer instantiated: no DB hits

serializer.is_valid():
    loops over each resource in list:
        -loops over each field:
            -run field.to_internal_value()
                for PKRelatedFields: fetch the item by PK
            -run validate_field()

serializer.save() --> calls into ListSerializer.create() (or update())
    for writeable RelatedFields: validated_data has already-fetched objects
    For nested fields: the validated data remains as python primitives

    inside manager bulk_create
        Accessing related objects: make sure they have been prefetched to avoid generating queries!

        For Many to one relations, just create related objects with self.pk
        for Many to Many, establish the junction with related_manager.set()
            Each save() does INSERT INTO
    
After serializer.save() finished
Upon accessing serializer.data to send Response:
    Start serializing the created/retrieved objects:
        Make sure related properties are prefetched!
            -> for List/Retrieve, the view should prefetch
            -> for Create/Update, the Manager should return the pre-fetched list or queryset