from django import forms
from balsam.core.models import ApplicationDefinition, BalsamJob


class AddAppForm(forms.ModelForm):
    class Meta:
        model = ApplicationDefinition
        fields = '__all__'

    def __init__(self, *args, **kwargs):
        super(AddAppForm,self).__init__(*args,**kwargs)
        self.fields['preprocess'].required = False
        self.fields['postprocess'].required = False
        self.fields['envscript'].required = False


class AddBalsamJobForm(forms.ModelForm):
    class Meta:
        model = BalsamJob
        fields = '__all__'

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('label_suffix','')
        super(AddBalsamJobForm,self).__init__(*args,**kwargs)
        self.use_required_attribute = True
        for field in self.fields:
            self.fields[field].widget.attrs['rows'] = 1
            if field in ['lock','stage_in_url','stage_out_url',
                         'environ_vars','args','user_workdir',
                         'stage_out_files','data']:
                self.fields[field].required = False
            if self.fields[field].required:
                self.fields[field].label_suffix = '*'
        # self.fields['cpu_affinity'].widget.attrs['rows'] = 1
        # self.fields['description'].initial = ''
        # self.fields['wall_time_minutes'].initial = 60
        # self.fields['coschedule_num_nodes'].required = False
