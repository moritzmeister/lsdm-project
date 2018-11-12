# lsdm-project
### Authors: [Gioele Bigini](https://github.com/BigG-DSC), [Moritz Meister](https://github.com/moritzmeister/)

### Project description:
Please see the FLink Project_2018.pdf for the description of the project and functionalities of this program.
The program is optimized to run in a parallelized way. You just need to specify the available taskslots in your flink cluster with the *-p* parameter when submitting the job to the cluster.

### How to run:
To execute the program, cd into the *flinkProgram* directory and run the following commands in order to submit the program to your running Flink cluster:

```
mvn clean package -Pbuild-jar
flink run -p 10 -c master2018.flink.VehicleTelematics target/flinkProgram-1.0-SNAPSHOT.jar $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER
```

### Requirements:
- Oracle Java 8 
- Apache Flink 1.3.2
