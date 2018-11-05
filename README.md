# Flink Project - Data Streaming

> **Authors:** Mehdi El Idrissi Boutaher & Ricardo Mirón Torres </br>
> **Univesity:** Universidad Politécnica de Madrid </br>
> **Program:** Máster Universitario en Software y Sistemas </br>
> **Subject:** Cloud Computing And Big Data Ecosystems Design

## About
Drivers, fleet owners, transport operations, insurance companies are stakeholders of vehicle monitoring applications which need to have analytical reporting on the mobility patterns of their vehicles, as well as real-time views in order to support quick and efficient decisions towards eco-friendly moves, cost-effective maintenance of vehicles, improved navigation, safety and adaptive risk management.

Vehicle sensors do continuously provide data, while on-the-move, they are processed in order to provide valuable information to stakeholders. Applications identify speed violations, abnormal driver behaviors, and/or other extraordinary vehicle conditions, produce statistics per driver/vehicle/fleet/trip, correlate events with map positions and route, assist navigation, monitor fuel consumptions, and perform many other reporting and alerting functions.

In this project we consider that each vehicle reports a position event every 30 seconds with the following format: _Time, VID, Spd, XWay, Lane, Dir, Seg, Pos_

The goal of this project is to develop a Java program using _Flink_ implementing the following functionality:
- **Speed Radar:** detects cars that overcome the speed limit of 90 mph
- **Average Speed Control:** detects cars with an average speed higher than 60 mph between
segments 52 and 56 (both included) in both directions. If a car sends several reports on segments 52 or 56, the ones taken for the average speed are the ones that cover a longer distance.
- **Accident Reporter:** detects stopped vehicles on any segment. A vehicle is stopped when it reports at least 4 consecutive events from the same position.

See the full specification [here](resources/Flink Project_2018.pdf).

## SetUp
**Requirements:**
- Java JDK 8
- Maven
- Flink 1.3.2
- Ubuntu or GNU/Linux

**Building & running:**
1. Start Flink, run the following command in the directory where Flink is located (i.e. _home/user/Downloads/flink-1.3.2_)

`bin/start-cluster.sh`

You can check the running tasks in `localhost:8081`

2. Build the JAR file running the following command in the project directory (i.e. _home/user/Documents/VehicleTelematics_)

`mvn clean install -Pbuild-jar`

This will create a new directory called _target_ inside the project `VehicleTelematics/target` with the JAR file inside.

3. Run the following command in the project directory to execute the application.

`flink run -c $CLASS_NAME target/$YOUR_JAR_FILE $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER`

You may need to additionally specify the complete path of Flink and add the class library, like the following example:

`/home/user/Downloads/flink-1.3.0/bin/flink run -c flinkproject.VehicleTelematics target/VehicleTelematic-1.0-SNAPSHOT.jar /home/user/VehicleTelematics/resources/input.csv /home/user/VehicleTelematics/resources/`

## Program Structure
The first thing a Flink application needs to do is set up its execution environment. Our `VehicleTelematics` class contains the _Main_ class which contains all the configuration options as well as setting the program parallelism. It retrieves the other methods,  

**Vehicle Telematics**

- Set up the execution environment
- Read an input stream
- Apply transformations (import _SpeedRadar, AvgSpeed & AccidentReporter_)
- Output the result
- Execute

---

**Speed Radar**
- Filter cars over 90 mph (`Filter[Spd]`)

**Average Speed Control**
- Filter segment 52 to 56 (`Filter[Seg]`)
- Keyed stream (`KeyBy[VID, Dir]`)
> Timestamps and watermarks for event-time ¿?

- Start session window (`EventTimeSessionWindows`)
- Check segment completion (`UDF`)
> Cars that do not complete the segment (52-56) are not taken into account.

- Calculate average speed (`UDF`)
> The average speed is calculated as the distance divided by the time spent to drive that distance. `AvgSpeed = (FinalPos - InitialPos) / (FinalTime2 - InitialTime)`

**Accident Reporter**


## References
