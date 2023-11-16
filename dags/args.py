from datetime import datetime, timedelta

default_args = {
    "owner": "Ngoc_Thinh",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2023, 1, 1),
}
