import multiprocessing
import signal


class BalsamService(multiprocessing.Process):
    def __init__(self, client, *args, service_period=1.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        self._exit_event = multiprocessing.Event()
        self._service_period = service_period

    def sig_handler(self, signum, stack):
        self._exit_event.set()

    def run(self, *args, **kwargs):
        signal.signal(signal.SIGINT, self.sig_handler)
        signal.signal(signal.SIGTERM, self.sig_handler)
        self.client.close_session()
        while True:
            self.run_cycle()
            try:
                should_exit = self._exit_event.wait(timeout=self._service_period)
            except multiprocessing.TimeoutError:
                pass
            else:
                if should_exit:
                    break
        self.cleanup()

    def run_cycle(self):
        pass

    def cleanup(self):
        pass
