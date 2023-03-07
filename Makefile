start-airflow:
	cd ./airflow && docker compose up -d --no-deps --build

start-gko:
	PG_PASSWORD=${PG_PASSWORD} docker compose up -d --no-deps --build

add-conn-pg:
	docker exec airflow-webserver bash -c "airflow connections add \"PG_GKO\" --conn-json '{\
    \"conn_type\": \"postgres\",\
    \"login\": \"gko\",\
    \"password\": \"${PG_PASSWORD}\",\
    \"host\": \"172.16.0.30\",\
    \"port\": \"55432\",\
    \"schema\": \"gko\",\
    \"extra\": {\"sslmode\": \"disable\"}\
}'"

5-add-variables:
	docker exec de-project-airflow bash -c "airflow variables import /lessons/variables.json"

6-add-conn-api-delivery:
	docker exec de-project-airflow bash -c "airflow connections add \"API_DELIVERY\" --conn-json '{\
    \"conn_type\": \"http\",\
    \"host\": \"d5d04q7d963eapoepsqr.apigw.yandexcloud.net\",\
    \"schema\": \"https\",\
    \"extra\": {\"X-Nickname\": \"ragimatamov\", \"X-Cohort\": \"6\", \"X-API-KEY\": \"${api-key}\"}\
}'"

stop-airflow:
	cd ./airflow && docker compose down -v

stop-gko:
	docker compose down -v
