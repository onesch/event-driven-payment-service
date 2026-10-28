from fastapi import FastAPI

from src.payment_service.api.v1.payment import router as payment_v1_router

def create_app() -> FastAPI:
    """
    Function for create FastAPI app instance.

    Returns:
        FastAPI: Configured FastAPI app instance.
    """
    app = FastAPI()

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    # Routers
    app.include_router(payment_v1_router, prefix="/api/v1")

    return app

app = create_app()
