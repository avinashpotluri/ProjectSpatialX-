package Sequential;

import EMSpatialJoin.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapperForAddingD extends Mapper<LongWritable, Text, LongWritable, ABCDJoinTuple> {

   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
        String[] line = value.toString().trim().split(",");
        int JoinType=0;
        int rowNum1 = Integer.parseInt(line[0]);
	int relationIndex1 = Integer.parseInt(line[1]);
	double x11 = Double.parseDouble(line[2]);
	double y11 = Double.parseDouble(line[3]);
	double x12 = Double.parseDouble(line[4]);
	double y12 = Double.parseDouble(line[5]);
	//Rectangle R1 = new Rectangle(rowNum1, relationIndex1,x11,y11,x12,y12);
	int rowNum2 = 0 ;//Integer.parseInt(line[7]);
	int relationIndex2 = 0;// Integer.parseInt(line[8]);
	double x21 = 0.0; 
	double y21 = 0.0; 
	double x22 = 0.0;
	double y22 = 0.0;
                         
        int rowNum3 = 0;
	int relationIndex3 = 0;
	double x31 = 0.0;
	double y31 = 0.0;
	double x32 = 0.0;
	double y32 = 0.0;
        
        int rowNum4 = 0;
	int relationIndex4 = 0;
	double x41 = 0.0;
	double y41 = 0.0;
	double x42 = 0.0;
	double y42 = 0.0;
        int abcType=2;
	
        ABCDJoinTuple jointuple = new ABCDJoinTuple(JoinType,rowNum1,relationIndex1,x11,y11,x12,y12,rowNum2,relationIndex2,x21,y21,x22,y22,rowNum3,relationIndex3,x31,y31,x32,y32,rowNum4,relationIndex4,x41,y41,x42,y42);
        
      
                context.write(key, jointuple);
            }
        }
 
