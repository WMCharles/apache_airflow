docker network connect airflow_docker_default kifaru-db

docker exec -it airflow_docker-airflow-scheduler-1 bash

airflow connections add "farmlytics_postgres" \
    --conn-type "postgres" \
    --conn-host "kifaru-db" \
    --conn-port "5432" \
    --conn-login "postgres" \
    --conn-password "hsgahwsb23@" \
    --conn-schema "farmlytics"
