package AllRepSpatialJoin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapper extends Mapper<LongWritable, Text, LongWritable, Rectangle> 
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
        Configuration conf = context.getConfiguration();
      
        int max = conf.getInt("gridMax",0);
        int numOfReducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
        double cellWidth =  (double) max / numOfReducersPerRow;
        double cellHeight =  (double) max / numOfReducersPerRow;

        String[] line = value.toString().trim().split(",");

        double[] x = {Double.parseDouble(line[2]), Double.parseDouble(line[4])};
        double[] y = {Double.parseDouble(line[3]), Double.parseDouble(line[5])};
        int relation = 0;
        if (line[1].equals("A")) 
        {
            relation = 1;
        }
        if (line[1].equals("B")) 
        {
            relation = 2;
        }
        if (line[1].equals("C")) 
        {
            relation = 3;
        }
        if (line[1].equals("D")) 
        {
            relation = 4;
        }
        Rectangle r = new Rectangle(Integer.parseInt(line[0]), relation, x[0], y[0], x[1], y[1]);

        int x1 = (int) Math.floor(x[0] / cellWidth); //it gives cell Col
        int y1 = (int) Math.floor(y[0] / cellHeight);   //it gives cell Row
        int cellCol=y1;
        int cellRow=x1;
        int key1 = 0;
     //   int x2 = (int) Math.floor(x[1] / cellWidth);
         //  int y2 = (int) Math.floor(y[1] / cellHeight);

       // for (int i = x1; i <= x2; i++){
        //     for (int j = y1; j <= y2; j++) {
      //  int mapkey = ((cellRow * numOfReducersPerRow) + cellCol);   //it gives reducer no (cell no)
            //    System.out.println("Mapper1" + mapkey + " " + i + " " + j );
            
        for(int i= cellRow; i< numOfReducersPerRow ; i++) 
        {
            for(int j=cellCol; j< numOfReducersPerRow ; j++) 
            {
                key1= (i * numOfReducersPerRow) + j;
                context.write(new LongWritable(key1), r);
            }
        }
                
    }
}
