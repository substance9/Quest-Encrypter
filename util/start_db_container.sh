#!/bin/bash

echo "Creating Volume"
docker volume create quest_db_vol

echo "Starting Container"
docker run --name quest_db --rm --volume=quest_db_vol:/var/lib/postgresql/data -p 10009:5432 --shm-size=8G -e POSTGRES_PASSWORD=password -d postgres:12.1 -N 1000

echo "Creating DB"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -c "create database tippers_quest;"