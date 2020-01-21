import os
from .base import *

if os.environ.get('DJANGO_SETTINGS_PROD'):
    from .prod import *
else:
    from .dev import *
