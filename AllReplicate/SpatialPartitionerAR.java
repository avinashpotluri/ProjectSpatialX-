package AllReplicate;

import EMSpatialJoin.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SpatialPartitionerAR extends 
   Partitioner<LongWritable, JoinTuple> {

	@Override
	public int getPartition(LongWritable key, JoinTuple value, int numberOfPartitions) {
		long num = key.get();
		return Math.abs((int) (num % numberOfPartitions));
	}
}