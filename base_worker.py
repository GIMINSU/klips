class BaseWorker:
    def __init__(self, spec, *args, **kwargs):
        self.mgr = mgr