import multiprocessing
import time
from waitress import serve
import pytest
from normlite.proxy.client import create_proxy_client
from normlite.proxy.server import create_app

def run_server(port):
    app = create_app()
    serve(app, host="127.0.0.1", port=port)

def _notify(request, msg: str):
    """Always write messages to pytest terminal output."""
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    if reporter:
        reporter.write_line(msg)

@pytest.fixture(scope="session")
def live_server(request):
    port = 5001
    process = multiprocessing.Process(
        target=run_server, 
        args=(port,), 
        daemon=True)
    process.start()
    time.sleep(1)  # crude but ensures the server is up
    _notify(request, f"Started NormliteProxyServer (PID={process.pid})")
    yield f"http://127.0.0.1:{port}"
    process.terminate()
    process.join()
    _notify(request, f"Stopped NormliteProxyServer (PID={process.pid})")

@pytest.fixture(scope='session')
def flask_client():
    app = create_app()
    yield app.test_client()

def test_connect_with_live_server(live_server):
    """Check whether the proxy server is alive and can process requests."""
    proxy_client = create_proxy_client(base_url=live_server)
    assert proxy_client
    assert proxy_client.base_url == 'http://127.0.0.1:5001'
    response = proxy_client.connect()

    assert response.status_code == 200
    assert response.json()['message'] == 'Proxy server is alive'

def test_connect_with_test_server(flask_client):
    proxy_client = create_proxy_client(flask_client=flask_client)
    assert proxy_client
    #assert proxy_client.base_url == 'http://127.0.0.1:5001'
    response = proxy_client.connect()

    assert response.status_code == 200
    assert response.json()['message'] == 'Proxy server is alive'
