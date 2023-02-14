import warnings
warnings.filterwarnings("ignore", message="You're running the worker with superuser privileges")

from app import celery, create_app

app = create_app()
app.app_context().push()
