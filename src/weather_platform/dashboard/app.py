import json
import urllib.error
import urllib.request

from dash import Dash, html, page_container
from flask import Response

from weather_platform.config import API_BASE_URL, DASH_HOST, DASH_PORT


app = Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
)

server = app.server



@server.route("/health/data")
def data_health():
    """
    Public data-freshness check for external uptime monitors.

    Only Dash is exposed through Nginx, so this relays the API's
    /health/data result: 200 when fresh, 503 when stale or unreachable.
    """
    try:
        with urllib.request.urlopen(
            f"{API_BASE_URL}/health/data",
            timeout=10,
        ) as response:
            body, status = response.read(), response.status
    except urllib.error.HTTPError as exc:
        body, status = exc.read(), exc.code
    except (urllib.error.URLError, TimeoutError, OSError):
        body = json.dumps({"detail": "Weather API unreachable."}).encode()
        status = 503

    return Response(body, status=status, mimetype="application/json")


app.title = "Real-Time Weather Analytics Platform"

app.layout = html.Div(
    [
        page_container,
    ]
)


if __name__ == "__main__":
    app.run(
        host=DASH_HOST,
        port=DASH_PORT,
        debug=True,
    )
