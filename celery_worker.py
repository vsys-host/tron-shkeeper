from app.wallet_encryption import wallet_encryption
wallet_encryption.setup_encryption()

import warnings
warnings.filterwarnings("ignore", message="You're running the worker with superuser privileges")

from app import celery, create_app

app = create_app()
app.app_context().push()
