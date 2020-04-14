#!/bin/bash

echo "Creating Volume"
docker container stop quest_db

echo "Starting Container"
docker volume remove quest_db_vol