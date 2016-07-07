
from __future__ import absolute_import, unicode_literals

import copy
from itertools import chain
import warnings

#from django.conf import settings
#from django.forms.util import flatatt, to_current_timezone
#from django.utils.datastructures import MultiValueDict, MergeDict
#from django.utils.html import conditional_escape, format_html
#from django.utils.translation import ugettext_lazy
#from django.utils.encoding import force_text, python_2_unicode_compatible
#from django.utils.safestring import mark_safe
#from django.utils import datetime_safe, formats, six
#from django.utils.six.moves.urllib.parse import urljoin

from django import forms
from django.forms.widgets import CheckboxInput

from argo import models
import logging
logger = logging.getLogger(__name__)

list_of_tuples = []
for state in models.STATES:
   list_of_tuples.append( (state.name,state.name) )
ALL_STATES = tuple(list_of_tuples)

class JobDisplayForm(forms.Form):
   state             = forms.ChoiceField(required=True,widget=forms.Select,choices=ALL_STATES)
   group_identifier  = forms.CharField(required=True,widget=forms.TextInput(attrs={'size':150}))
   email             = forms.CharField(required=True,widget=forms.TextInput(attrs={'size':150}))
   confirm_delete    = forms.BooleanField(required=False)

   def __init__(self, current_subjob_choices=[('-1','no choices')], *args, **kwargs):
      super(JobDisplayForm, self).__init__(*args, **kwargs)
      self.fields['current_subjob'] = forms.ChoiceField(choices=current_subjob_choices)
