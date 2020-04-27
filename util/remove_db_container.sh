#!/bin/bash

echo "Stop Container"
docker container stop quest_db

echo "Remove Vol"
docker volume remove quest_db_vol
