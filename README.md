# spark_cme
Distributed numerical solution of the Chemical Master Equation using Apache Spark

This is a demonstration of the Apache Spark framework, that is used to solve the Chemical Master Equation for
2 chemical species in a chemical network containing 4 reactions.
The state space is represented by tupels of (set of copy number,  probability of the state) and are parsed
as strings between mappers and reducers.
While the mapper is responsible for computing the time step using a simple first-order numerical procedure,
the reducer makes sure that probabiities for each feasible tupel are added up.
The code can be easily extended with e.g. a filter, which truncates states with very low probability, which
do not contribute much to the solution.
