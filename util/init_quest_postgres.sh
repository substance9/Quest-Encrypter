#!/bin/bash

echo "Delete existing table for the same duration"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -d tippers_quest -q -f ./table_drop.sql

echo "Initiate Log Schema"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -d tippers_quest -q -f ./table_creation.sql
