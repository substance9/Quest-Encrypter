## Software Requirements

**Supported OS**: Linux / macOS

**Required Softwares**:

- Bash
- Gradle: (version: 6.0+)
- Java (JDK): (version: 11+)
- Docker (version: 19+)
- Python 3.6.9
- psql: PostgreSQl client program



## Brief Intro

Encrypter Module reads from the Wi-Fi data CSV files, encrypt data and ingest the data into the PostgreSQL DB. This project is implemented in Java. It stores the encrypted data in docker version of PostgreSQL DB.


## Run the experiment

Please follow the following steps to set up the environment and run the experiments:

1. Create the following folder in the project directory (if not exist):
   - `./results/`

2. Initialize Docker volume for persistent DB storage and start PostgreSQL DB container
   - `cd util`
   - `./start_db_container.sh` 

3. Setup correct path variables according to the path where you put the project directory root:
   - in `./run_exp.py`: 
     - Modify `PROJECT_DIR_PATH` variable: the absolute path to the project root folder
     - Modify `DB_PORT` variable: The port that will be used for PostgreSQL DB port

4. Modify the experiment parameters in `./run_exp.py` at the bottom of the file (arguments in `runexp()` function)

5. Run the experiment by executing: `./run_exp.py`


