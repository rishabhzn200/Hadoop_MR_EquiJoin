# Hadoop_MR_EquiJoin
Performing EquiJoin of two tables using Hadoop MapReduce

<h3>The configuration of a new MapReduce Job:</h3>
Setting the Mapper class to JoinMapper.class<br/>
Setting the Reducer class to JoinReducer.class<br/>


<h3>Map Phase:</h3>
JoinMapper class implements this phase. It extends the Mapper class and overrides the map function. Each Map function gets the single input line from the input source.<br/>
Input line format is:<br/>
Tablename, JoinCol, Col2, col2.....<br/>

The input line is split using ',' as the delimiter and the value of JoinCol (which is at index 1) are set as the output key of the map. The value of map function is the whole input line with no change.
The output of the map function is (Key, Value) pairs where key = join columns id and value = whole input line


<h3>Shuffling/Combining Phase:</h3>
This phase is taken care of internally. This phase takes the output of Map phase (key, values) as input and groups together the values with the similar key. So, this phase creates a key, Iterable of values as output. Key is the unique key and Iterable of values is the list of values with the same key. This output is the input to the reducer phase.

<h3>Reduce Phase:</h3>
JoinReducer class implements this phase. It extends the Reducer class and overrides the reduce function. The input to the reducer function is the key and list of values. Here the key is the unique id and list of values is the Iterable over the input lines having the same key(Join column id in this case).<br/>
Each reducer gets the key and the Iterable of values(input lines with the same key). The values in the Iterable comes from both the tables. So, the next step is to create two different Lists(Tables) from the Iterable over values. This is done by splitting each value in Iterable(the input line) using the ',' as a delimiter and extracting the table name which is at index 0. This table name helps us to create two different lists. A boolean flag is used to help us with this task.<br/>
Once the two tables are created, the next step is to take the Cartesian product of the two tables. The result of the cartesian product gives us the join over that key. This reduce phase happens for each key. And the output is written using the input context in the reduce function.
