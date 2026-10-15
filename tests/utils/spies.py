"""Test spies that wrap a Notion client."""


class QueryParamsRecordingClient:
    """Transparent proxy that records the ``query_params`` of every
    ``data_sources.query`` request.

    The request that reaches Notion is the observable: the Planner copies
    ``ctx.parameters`` and never changes them in place.
    """

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.query_params_seen = []

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "data_sources" and request == "query":
            self.query_params_seen.append(query_params)
        return self._wrapped(endpoint, request, path_params, query_params, payload)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class PagesUpdateRecordingClient:
    """Transparent proxy that records every ``pages.update`` request.

    Each entry is the ``(path_params, payload)`` pair that reaches Notion.
    A test reads ``len(updates_seen)`` at a chosen moment to see how many
    writes were sent by then.
    """

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.updates_seen = []

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "pages" and request == "update":
            self.updates_seen.append((path_params, payload))
        return self._wrapped(endpoint, request, path_params, query_params, payload)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class QueryPayloadRecordingClient:
    """Transparent proxy that records the ``payload`` of every
    ``data_sources.query`` request.

    A test reads the pushed ``filter`` from the payload that reaches Notion.
    The in-memory client has no nesting cap, so only the payload shows a
    filter that the real API rejects.
    """

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.payloads_seen = []

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "data_sources" and request == "query":
            self.payloads_seen.append(payload)
        return self._wrapped(endpoint, request, path_params, query_params, payload)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)
