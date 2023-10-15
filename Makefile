COMPOSE=docker compose
AIRFLOW_INIT_SERVICE=airflow-init
AIRFLOW_SERVICES=airflow-webserver airflow-triggerer airflow-scheduler
init:
	${COMPOSE} up ${AIRFLOW_INIT_SERVICE}

run:
	${COMPOSE} up -d

stop:
	${COMPOSE} stop ${AIRFLOW_SERVICES} ${AIRFLOW_INIT_SERVICE}

purge:
	${COMPOSE} down -v