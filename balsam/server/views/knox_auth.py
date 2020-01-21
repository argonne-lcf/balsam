from knox.views import LoginView as KnoxLoginView
from rest_framework.authentication import BasicAuthentication

class LoginView(KnoxLoginView):
    authentication_classes = [BasicAuthentication]
