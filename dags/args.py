from datetime import datetime, timedelta

default_args = {
    "owner": "Ngoc_Thinh",
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
    "start_date": datetime(2023, 1, 1),
}
