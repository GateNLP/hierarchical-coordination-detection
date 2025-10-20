# only need to do the node build once, even if we're building multi-arch
FROM --platform=$BUILDPLATFORM node:21-slim AS nodebuilder
COPY ui/package.json ui/package-lock.json /app/
WORKDIR /app
RUN npm ci --unsafe-perm
COPY ui/ /app
RUN npm run build

FROM python:3.10-slim AS backend
# backend container with the Python app but no frontend files

# Install tini and create an unprivileged user
RUN apt-get update && \
    apt-get install -y \
      tini \
    && rm -rf /var/lib/apt/lists/* && \
    addgroup --gid 1001 "coordination" && \
    adduser --disabled-password --gecos "Coordination User,,," \
      --home /coordination --ingroup coordination --uid 1001 coordination

# Everything from here down runs as the unprivileged user account
USER 1001:1001

WORKDIR /coordination

# Create a Python virtual environment for the dependencies
# We mount a cache which we use for pip. This means that
# repeat builds are much much quicker as we don't need to
# re-download all the dependencies. In this instance we've
# used a none default directory so that ~/.cache doesn't end
# up owned by root and so that when we run the classifier
# torch can cache it's downloads in the normal place
RUN --mount=type=cache,target=/coordination/.pip-cache,uid=1001 python -mvenv venv

# pyproject.toml is needed to install the package
# https://setuptools.pypa.io/en/stable/userguide/miscellaneous.html
COPY --chown=1001:1001 service/pyproject.toml /coordination/

RUN --mount=type=cache,target=/coordination/.pip-cache,uid=1001,gid=1001 venv/bin/pip \
  --cache-dir /coordination/.pip-cache install \
  --extra-index-url https://repo.gate.ac.uk/repository/pip-local/simple \
  --editable . \
  && rm -rf *.egg-info

# Copy the rest of the code
COPY --chown=1001:1001 service/ /coordination/
RUN --mount=type=cache,target=/coordination/.pip-cache,uid=1001,gid=1001 venv/bin/pip \
  --cache-dir /coordination/.pip-cache install \
  --extra-index-url https://repo.gate.ac.uk/repository/pip-local/simple \
  --editable .

ENV WORKERS=1 WEBAPP_PORT=5000 ELASTIC_CLIENT_APIVERSIONING=1

ENTRYPOINT ["./docker-entrypoint.sh"]

FROM ghcr.io/static-web-server/static-web-server:2 AS frontend
# frontend-only container served by a static web server
COPY --from=nodebuilder /app/dist /dist
COPY ui/sws-config.toml /sws-config.toml
EXPOSE 8088
USER 1001:1001
CMD ["-w", "/sws-config.toml"]

FROM backend AS combined
# combined container that runs the backend and also serves the frontend
# static files using whitenoise

COPY --from=nodebuilder /app/dist static/
ENV COORDINATION_SERVE_STATIC=1
