from flask import Flask
from flask_compress import Compress
import os
from logging.config import dictConfig

from .blueprints.job_routes import job_bp
from .blueprints.post_routes import post_bp

# Import existing route functions for backward compatibility
# These maintain the original URL structure while using the new blueprint functions
from .blueprints.job_routes import (
    process as job_process,
    status as job_status,
    getJob,
    sendResult,
    sendGraphResult,
)
from .blueprints.post_routes import sendPost

dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,  # for Gunicorn compatibility
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            },
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(
    __name__,
    static_url_path="",
    static_folder="static",
)
Compress(app)


if "COORDINATION_SERVE_STATIC" in os.environ:
    from whitenoise import WhiteNoise

    app.wsgi_app = WhiteNoise(
        app.wsgi_app,
        root="static/",
        # treat anything under /assets as "immutable", as those have hashed file names
        immutable_file_test=r"assets/[^/]+$",
        # when a request comes in that maps to a directory name, serve index.html
        # from that directory
        index_file=True,
    )


# Register blueprints
app.register_blueprint(job_bp)
app.register_blueprint(post_bp)


@app.route("/healthz")
def healthcheck():
    return dict(alive=True)