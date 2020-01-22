from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import MODELS

for key, model in MODELS.items():
    if key == 'User':
        admin.site.register(model, UserAdmin)
    else:
        admin.site.register(model)
