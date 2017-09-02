package Sequential;

import EMSpatialJoin.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapperSeqForABCD extends Mapper<LongWritable, Text,LongWritable , ABCDJoinTuple>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String words[] = value.toString().split(",");
	String[] subWords = words[0].split("\\s+");
			   
	int JoinType=0;
	JoinType =Integer.parseInt(subWords[subWords.length-1]);
	
        int rowNum1 = Integer.parseInt(words[23]);
	int relationIndex1 = Integer.parseInt(words[26]);
	double x11 = Double.parseDouble(words[29]);
	double y11 = Double.parseDouble(words[30]);
	double x12 = Double.parseDouble(words[31]);
	double y12 = Double.parseDouble(words[32]);
	//Rectangle R1 = new Rectangle(rowNum1, relationIndex1,x11,y11,x12,y12);
	int rowNum2 = Integer.parseInt(words[24]);
	int relationIndex2 = Integer.parseInt(words[27]);
	double x21 = Double.parseDouble(words[33]);
	double y21 = Double.parseDouble(words[34]);
	double x22 = Double.parseDouble(words[35]);
	double y22 = Double.parseDouble(words[36]);
			   
        int rowNum3 = Integer.parseInt(words[25]);
	int relationIndex3 = Integer.parseInt(words[28]);
	double x31 = Double.parseDouble(words[37]);
	double y31 = Double.parseDouble(words[38]);
	double x32 = Double.parseDouble(words[39]);
	double y32 = Double.parseDouble(words[40]);
        
        int rowNum4 = Integer.parseInt(words[3]);
	int relationIndex4 = Integer.parseInt(words[6]);
	double x41 = Double.parseDouble(words[9]);
	double y41 = Double.parseDouble(words[10]);
	double x42 = Double.parseDouble(words[11]);
	double y42 = Double.parseDouble(words[12]);
       
        int abcType=1;
	//Rectangle R2 = new Rectangle(rowNum2, relationIndex2,x21,y21,x22,y22);
	
        
        ABCDJoinTuple jointuple = new ABCDJoinTuple(JoinType,rowNum1,relationIndex1,x11,y11,x12,y12,rowNum2,relationIndex2,x21,y21,x22,y22,rowNum3,relationIndex3,x31,y31,x32,y32,rowNum4,relationIndex4,x41,y41,x42,y42);		 
                        int mapkey = 1;
                        //     System.out.println("Mapper2 Reducer " + mapkey + " " + i + " " + j + " " + r.toString());
                        context.write(new LongWritable(mapkey), jointuple);
                    }
}                 