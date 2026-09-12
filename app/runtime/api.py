from app.runtime.bootstrap import configure_runtime, parse_runtime_arguments


def main() -> None:
    parse_runtime_arguments("fluxion-api", "Run the Fluxion API service.")
    settings = configure_runtime()
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_config=None,
    )
