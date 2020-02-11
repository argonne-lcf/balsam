from balsam import App


class Hello(App):
    template = 'echo "Hello, {{ name }}!"'
    name: str
