import os

MAX_EMAIL_RESULTS_PER_CALL = int(
    os.environ.get("MAX_EMAIL_RESULTS_PER_CALL", 50)
)
