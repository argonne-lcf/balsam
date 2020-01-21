from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import MODELS

admin.site.register(MODELS['User'], UserAdmin)
admin.site.register(MODELS['Site'])
admin.site.register(MODELS['App'])
admin.site.register(MODELS['Job'])
admin.site.register(MODELS['TransferTask'])
admin.site.register(MODELS['BatchJob'])
