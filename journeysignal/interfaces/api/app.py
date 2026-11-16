"""Read-only HTTP interface. Synchronous I/O runs in FastAPI's thread pool."""

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Annotated, cast

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.exc import SQLAlchemyError

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.persistence.queries import PostgresQueries
from journeysignal.adapters.telemetry.metrics import metrics
from journeysignal.application.queries import FindingFilter, QualityQuery, TimeWindow
from journeysignal.bootstrap import load_settings, make_engine
from journeysignal.domain.findings import Severity
from journeysignal.domain.journeys import JourneyDependency, JourneyDependencyRegistry
from journeysignal.domain.rules import RULE_IDS
from journeysignal.interfaces.api.schemas import (
    FindingPageResponse,
    HealthResponse,
    ImpactResponse,
    OverviewResponse,
    Problem,
)

logger = logging.getLogger(__name__)


def window_parameters(
    from_time: Annotated[datetime | None, Query(alias="from")] = None,
    to_time: Annotated[datetime | None, Query(alias="to")] = None,
) -> TimeWindow:
    end = to_time or datetime.now(UTC)
    try:
        return TimeWindow(from_time or end - timedelta(minutes=30), end)
    except ValueError as error:
        raise HTTPException(422, str(error)) from None


def queries(request: Request) -> QualityQuery:
    if not hasattr(request.app.state, "queries"):
        raise HTTPException(503, "Storage is not configured")
    return cast(QualityQuery, request.app.state.queries)


def create_app(
    ready_check: Callable[[], bool] | None = None,
    query: QualityQuery | None = None,
    registry: JourneyDependencyRegistry | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        engine = None
        if query is not None:
            application.state.queries = query
        elif ready_check is None:
            settings = load_settings()
            if settings.api_database_url:
                settings = settings.model_copy(update={"database_url": settings.api_database_url})
            engine = make_engine(settings)
            application.state.queries = PostgresQueries(engine)
            application.state.registry = registry or load_registry(settings.journey_config_path)
        if not hasattr(application.state, "registry"):
            application.state.registry = registry or load_registry()
        try:
            yield
        finally:
            if engine is not None:
                engine.dispose()

    app = FastAPI(
        title="JourneySignal",
        version="0.1.0",
        lifespan=lifespan,
        description="Synthetic event quality and configured potential journey exposure.",
        responses={
            400: {"model": Problem},
            404: {"model": Problem},
            422: {"model": Problem},
            500: {"model": Problem},
            503: {"model": Problem},
        },
    )

    def problem(status: int, detail: str) -> JSONResponse:
        titles = {
            400: "Bad request",
            404: "Not found",
            422: "Invalid request",
            503: "Service unavailable",
            500: "Internal server error",
        }
        return JSONResponse(
            {
                "type": "about:blank",
                "title": titles.get(status, "Request failed"),
                "status": status,
                "detail": detail,
            },
            status_code=status,
            media_type="application/problem+json",
        )

    @app.middleware("http")
    async def safe_error_boundary(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        try:
            return await call_next(request)
        except Exception:
            logger.error("internal_error", extra={"code": "internal_error"})
            return problem(500, "Unexpected service error")

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException) -> JSONResponse:
        return problem(error.status_code, str(error.detail))

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, error: RequestValidationError) -> JSONResponse:
        return problem(422, "Invalid query parameters; check types, limits and timestamp formats")

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request: Request, error: SQLAlchemyError) -> JSONResponse:
        return problem(503, "Storage is temporarily unavailable")

    @app.get("/health/live", response_model=HealthResponse)
    def live() -> HealthResponse:
        return HealthResponse(status="alive")

    @app.get("/health/ready", response_model=HealthResponse)
    def ready(request: Request) -> HealthResponse:
        available = ready_check() if ready_check else queries(request).ready()
        if not available:
            raise HTTPException(503, "Storage is unavailable or migrations are pending")
        return HealthResponse(status="ready")

    @app.get("/metrics", include_in_schema=False)
    def metric_output() -> Response:
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/v1/quality/overview", response_model=OverviewResponse)
    def overview(
        window: Annotated[TimeWindow, Depends(window_parameters)],
        store: Annotated[QualityQuery, Depends(queries)],
    ) -> OverviewResponse:
        return OverviewResponse.model_validate(asdict(store.overview(window)))

    def page(
        store: QualityQuery,
        filters: FindingFilter,
        limit: int,
        cursor: str | None,
    ) -> FindingPageResponse:
        try:
            return FindingPageResponse.model_validate(
                asdict(store.findings(filters, limit, cursor))
            )
        except ValueError:
            raise HTTPException(400, "Invalid cursor or filters do not match cursor") from None

    @app.get("/api/v1/quality/findings", response_model=FindingPageResponse)
    def findings(
        store: Annotated[QualityQuery, Depends(queries)],
        rule_id: Annotated[str | None, Query(max_length=16)] = None,
        severity: Severity | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query(max_length=512)] = None,
    ) -> FindingPageResponse:
        if rule_id is not None and rule_id not in RULE_IDS:
            raise HTTPException(422, "Unknown rule ID")
        return page(store, FindingFilter(rule_id=rule_id, severity=severity), limit, cursor)

    @app.get("/api/v1/journeys", response_model=list[JourneyDependency])
    def journeys(request: Request) -> tuple[JourneyDependency, ...]:
        return cast(JourneyDependencyRegistry, request.app.state.registry).journeys

    @app.get("/api/v1/journeys/{journey_id}/impact", response_model=ImpactResponse)
    def impact(
        request: Request,
        journey_id: str,
        window: Annotated[TimeWindow, Depends(window_parameters)],
        store: Annotated[QualityQuery, Depends(queries)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query(max_length=512)] = None,
    ) -> ImpactResponse:
        catalog = cast(JourneyDependencyRegistry, request.app.state.registry)
        if journey_id not in {j.journey_id for j in catalog.journeys}:
            raise HTTPException(404, "Unknown journey")
        counts = store.impact_counts(journey_id, window)
        finding_page = page(
            store, FindingFilter(journey_id=journey_id, window=window), limit, cursor
        )
        return ImpactResponse(
            journey_id=journey_id,
            window=asdict(window),
            affected_events=counts.affected_events,
            finding_count=counts.findings,
            status="potential_data_quality_impact" if counts.findings else "no_findings_in_window",
            findings=finding_page,
        )

    @app.get("/api/v1/events/{event_id}/findings", response_model=FindingPageResponse)
    def event_findings(
        event_id: str,
        store: Annotated[QualityQuery, Depends(queries)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query(max_length=512)] = None,
    ) -> FindingPageResponse:
        if len(event_id) > 100:
            raise HTTPException(422, "Event ID exceeds contract limit")
        return page(store, FindingFilter(event_id=event_id), limit, cursor)

    return app


app = create_app()
