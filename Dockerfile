FROM ghcr.io/astral-sh/uv:0.13.8@sha256:5cc1805e2426484abca4ad46ea783bab281fdb225fd82d8ce6d66a81254bac85 AS uv
FROM python:3.13.18-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90 AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY journeysignal ./journeysignal
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13.18-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90
ENV PATH="/app/.venv/bin:$PATH" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN groupadd --gid 10001 journeysignal && useradd --uid 10001 --gid 10001 --no-create-home journeysignal
COPY --from=build /app/.venv /app/.venv
COPY migrations ./migrations
COPY alembic.ini ./
COPY scenarios ./scenarios
COPY LICENSE ./
USER 10001:10001
EXPOSE 8000
CMD ["uvicorn", "journeysignal.interfaces.api.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
