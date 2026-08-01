import structlog


def configure_logging() -> None:
    """
    Configura logging estructurado en formato JSON.
    """

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
    )


logger = structlog.get_logger()
