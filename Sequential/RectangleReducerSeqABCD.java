package Sequential;

import EMSpatialJoin.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleReducerSeqABCD extends Reducer<LongWritable, ABCDJoinTuple, LongWritable, ABCDJoinTuple> {

    public void reduce(LongWritable key, Iterable<ABCDJoinTuple> values, Context context) throws IOException, InterruptedException {
   
        for (ABCDJoinTuple t : values) {
            ABCDJoinTuple newt = new ABCDJoinTuple(t);
             context.write(key, newt);  
            }
        }
}