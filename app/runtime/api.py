from app.runtime.bootstrap import configure_runtime


def main() -> None:
    settings = configure_runtime()
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_config=None,
    )
