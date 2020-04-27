#!/usr/bin/python3
import os
import subprocess
import time
import pathlib
from datetime import datetime
from signal import signal, SIGINT
from sys import exit

PROJECT_DIR_PATH = "/home/ubuntu/Workspace/quest/encrypter"

table_creation_sql_file_name = "table_creation.sql"
table_drop_sql_file_name = "table_drop.sql"

DB_PORT = 10009
PROJECT_NAME_BASE = "quest_"

enc_table_creation_sql_template = \
"""
CREATE TABLE %s (
  ENCID TEXT NOT NULL,
  ENCU TEXT NOT NULL,
  ENCL TEXT NOT NULL,
  ENCCL TEXT NOT NULL,
  ENCD TEXT NOT NULL
);

CREATE TABLE %s_meta_opt1 (
  ENCD TEXT NOT NULL,
  ENCCOUNT TEXT NOT NULL
);

CREATE TABLE %s_meta_opt2 (
  ENCD TEXT NOT NULL,
  ENCL TEXT NOT NULL,
  ENCCOUNT TEXT NOT NULL
);
"""

enc_table_drop_sql_template = \
"""
DROP TABLE IF EXISTS %s;
DROP TABLE IF EXISTS %s_meta_opt1;
DROP TABLE IF EXISTS %s_meta_opt2;
"""

# def exit_handler(signal_received, frame):
#     # Handle any cleanup here
#     print('SIGINT or CTRL-C detected. Exiting gracefully')
#     for proc in proc_list:
#         proc.kill()
#     exit(0)

# signal(SIGINT, exit_handler)

def call_cmd(cmd_arr):
    print("Calling" + str(cmd_arr))
    subprocess.call(cmd_arr)

# def call_cmd_no_blocking(cmd_arr, output_file):
#     print("Calling" + str(cmd_arr))
#     proc = subprocess.Popen(cmd_arr, stdout=output_file)
#     return proc

def init_db(project_run_id):
    os.chdir(PROJECT_DIR_PATH + "/util")
    #Generate table creation SQL file for init db
    with open(table_creation_sql_file_name, "w") as table_creation_sql_file, open(table_drop_sql_file_name, "w") as table_drop_sql_file:
        table_creation_sql_file.write(enc_table_creation_sql_template % (project_run_id,project_run_id,project_run_id))
        table_drop_sql_file.write(enc_table_drop_sql_template % (project_run_id,project_run_id,project_run_id))
    print("Initiate DB for Quest Encrypter ")
    call_cmd(["./init_quest_postgres.sh",str(DB_PORT)])

def runexp(duration,experiment_id,enc_key,secret,enc_table_name,input_path,max_rows,output_path,opt1_enabled,opt2_enabled):
    #project_run_id = PROJECT_NAME_BASE + str(duration) + "_" + str(max_rows)
    init_db(enc_table_name)

    os.chdir(PROJECT_DIR_PATH)
    subprocess.call(["gradle","clean"])
    subprocess.call(["gradle","fatJar"])

    now = datetime.now()
    experiment_id = now.strftime("%Y-%m-%d-%H-%M-%S")

    output_dir = PROJECT_DIR_PATH + "/results/" + "dur_" + str(duration) + "|rows_" + str(max_rows)+ "|expID_" + experiment_id
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    db_port = DB_PORT
    call_cmd(["java", "-jar" ,"-Xmx64g", "build/libs/encrypter-all-0.1.jar", \
                        "-d", str(duration),\
                        "-x", experiment_id,\
                        "-k", enc_key,\
                        "-s", secret,\
                        "-p", str(db_port),\
                        "-n", enc_table_name,\
                        "-i", input_path,\
                        "-m", str(max_rows),\
                        "-o", output_dir,
                        "-a", str(opt1_enabled),
                        "-b", str(opt2_enabled)])

    # proc_list.append(encrypter_proc)

    # while(True):
    #     pass

#print("Please make sure the clean_exp.py is executed to remove the previous DB containers and volumes")

# Variables per experiment:
dur = 15 # IMPORTANT PARAMETER: delta duration in minutes
key = "tippersquest"
secret = "questsecret"
exp_id = "test"
max_rows= 10000000
enc_t_name = "quest_" + str(dur) + "_" + str(max_rows)
opt1_enabled = 1 # optimization 1: Per epoch max value counter. 0 for disable, 1 for enable
opt2_enabled = 0 # optimization 2: Per epoch and per location max value counter. 0 for disable, 1 for enable
in_path = "/home/ubuntu/Workspace/wifi_data/original_csv_202_days"
out_path = PROJECT_DIR_PATH + "/results"
runexp(duration=dur,experiment_id=exp_id,enc_key=key,secret=secret,enc_table_name=enc_t_name,input_path=in_path,max_rows=max_rows,output_path=out_path,opt1_enabled=opt1_enabled,opt2_enabled=opt2_enabled)
