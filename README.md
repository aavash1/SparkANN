# SparkANN
This is an implementation of Apache spark based processing of All-Nearest Neighbor Queries in a Road Network.
The centralized version of this work is already my repository named as SQORN.

All of the running codes will be in "distributedANN/src/main" folder

The required external JAR file is located in "distributedANN/externalJARs/ksp.jar", ksp.jar helps to find a shortest path between two vertices.
Includes YEN algorithm, huge thanks to the author. Original ksp.jar source can be found in "https://github.com/bsmock/k-shortest-paths".
