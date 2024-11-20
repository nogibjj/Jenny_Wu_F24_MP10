# Jenny_Wu_F24_MP10
[![CI](https://github.com/nogibjj/Jenny_Wu_F24_MP10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Jenny_Wu_F24_MP10/actions/workflows/cicd.yml)

# Mini Project 10: PySpark Data Processing
---
## Requirements (Mini Project 10):
1. Use PySpark to perform data processing on a large dataset
2. Include at least one Spark SQL query and one data transformation

## Steps
1. Prepare the necesary configuration files like the Dockerfile, devcontainer.json, Makefile, requirements.txt, and main.yml for GitHub Actions integration. Ensure that the requirements.txt lists all necessary packages (for example, pyspark).
2. Create a library folder with a lib.py file which contains the following functions:
   * An extract function, which pulls the above csv from the Internet, and writes some the following columns to a data/nypd_shooting.csv file.
   * Start and end functions which start and end a PySpark session, respectively.
   * A load function which loads the data from data/nypd_shooting.csv into PySpark.
   * A general query function which allows any Spark SQL query to be run against the data.
   * A transformation function which adds a column to the dataset with the cardinal direction of each borough.
4. Create a main.py script which calls all of the lib functions.
5. Create a test_main.py script which ensures that the lib functions run successfully.

## Results
The results of the outputs are saced in the pyspark_output.md file