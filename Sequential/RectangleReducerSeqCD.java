package Sequential;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleReducerSeqCD extends Reducer<LongWritable, ABCJoinTuple, LongWritable, Text> 
{
    public void reduce(LongWritable key, Iterable<ABCJoinTuple> values, Context context) throws IOException, InterruptedException 
    {
        ArrayList<ABCJoinTuple> lc = new ArrayList<>();
        ArrayList<ABCJoinTuple> ld = new ArrayList<>();
        
        Configuration conf = context.getConfiguration();
        
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        RectangleSeq cellRect = new RectangleSeq(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
     //   System.out.println("Reducer got ");
        
        for (ABCJoinTuple t : values) 
        {
            ABCJoinTuple newt = new ABCJoinTuple(t);
    //        System.out.println("ReducerBC----->>"+"\t"+ newt);
    //        System.out.println(t.r1RelationIndex+","+t.r2RelationIndex +","+t.r3RelationIndex);
            if(t.abcType==1)
            {
                if (t.r3RelationIndex == 3) 
                {
      //              System.out.println("frm mapper ABC in RED CD^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"+"\t"+newt );
                    lc.add(newt);
                }
            }
            else if(t.abcType==2)
            {
                if (t.r1RelationIndex == 4) 
                {
                    ld.add(newt);
      //              System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+"\t"+newt );
                } 
            }else 
                {
                }
        }
        join(cellNumber,lc, ld, 3, cellRect,context);
    }
    public void join(int CellNumber,ArrayList<ABCJoinTuple> lc, ArrayList<ABCJoinTuple> ld, int joinType, RectangleSeq cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        for (int i = 0; i < lc.size(); i++) 
        {
            ABCJoinTuple r2 = lc.get(i);
            for (int j = 0; j < ld.size(); j++) 
            {
                boolean overlaps = false;
                ABCJoinTuple r1 = ld.get(j);
                double oxl = 0, oxr = 0, oyb = 0, oyt = 0;
         //       System.out.println("CASE 1:Main ");
        //        System.out.println(r2.r3x1 +"<="+ r1.r1x1 +">"+r2.r3x2);
                //Case 1 : r1.x1 falls between r3.x1 and r3.x2
                if (r2.r3x1 <= r1.r1x1 && r2.r3x2 > r1.r1x1) 
                {   
                    //Caee 1.1
                    if (r2.r3y1 <= r1.r1y1 && r2.r3y2 > r1.r1y1 && r1.r1y2 > r2.r3y2) 
                    {
                        overlaps = true;
                        oxl = r1.r1x1;
                        oyb = r1.r1y1;
                        oxr = r2.r3x2;
                        oyt = r2.r3y2;
                    } // Case 1.2 
                    else if (r2.r3y1 <= r1.r1y1 && r2.r3y2 > r1.r1y1 && r1.r1y2 <= r2.r3y2) {
                        overlaps = true;
                        oxl = r1.r1x1;
                        oyb = r1.r1y1;
                        oxr = r2.r3x2;
                        oyt = r1.r3y2;
                    } // Case 1.3: 
                    else if (r2.r3y1 <= r1.r1y2 && r2.r3y2 > r1.r1y2 && r1.r1y1 < r1.r1y1) {
                        overlaps = true;
                        oxl = r1.r1x1;
                        oyb = r2.r3y1;
                        oxr = r2.r3x2;
                        oyt = r1.r1y2;
                    } //Case 1.4:
                    else if (r2.r3y2 < r1.r1y2 && r1.r1y1 < r2.r3y1) 
                    {
                        overlaps = true;
                        oxl = r1.r1x1;
                        oyb = r2.r3y1;
                        oxr = r2.r3x2;
                        oyt = r2.r3y2;
                    } 
                }
                // Case 2: r1.x2 is less than r3.x2 and r1.x2 is greaterThan r3.x2
                else if (r2.r3x1 <= r1.r1x2 && r2.r3x2 > r1.r1x2) 
                {       
                    //case 2.1:
                        if (r2.r3y1 <= r1.r1y1 && r2.r3y2 > r1.r1y1 && r1.r1y2 > r2.r3y2) {
                            overlaps = true;
                            oxl = r2.r3x1;
                            oyb = r1.r1y1;
                            oxr = r1.r1x2;
                            oyt = r2.r3y2;
                        }//case 2.2
                        else if (r2.r3y1 <= r1.r1y1 && r2.r3y2 > r1.r1y1 && r1.r1y2 <= r2.r3y2) {
                            overlaps = true;
                            oxl = r2.r3x1;
                            oyb = r1.r1y1;
                            oxr = r1.r1x2;
                            oyt = r1.r1y2;
                        } // Case 2.3: r2.y2 falls between r1.y1 and r1.y2
                        else if (r2.r3y1 <= r1.r1y2 && r2.r3y2 > r1.r1y2 && r1.r1y1 < r2.r3y1) {
                            overlaps = true;
                            oxl = r2.r3x1;
                            oyb = r2.r3y1;
                            oxr = r1.r1x2;
                            oyt = r1.r1y2;
                        }//case 2.4:
                        else if (r1.r1y1 < r2.r3y1 && r1.r1y2 > r2.r3y1) {
                            overlaps = true;
                            oxl = r2.r3x1;
                            oyb = r2.r3y1;
                            oxr = r1.r1x2;
                            oyt = r2.r3y2;
                        }
                } 
                //Case 3:
                else if (r1.r1x1 < r2.r3x1 && r1.r1x2 > r2.r3x2) 
                {
                    //case 3.1
                        if (r2.r3y1 <= r1.r1y1 && r2.r3y2 > r1.r1y1 && r1.r1y2 > r2.r3y2) {
                            overlaps = true;
                            oxl = r2.r3x1;
                            oyb = r1.r1y1;
                            oxr = r2.r3x2;
                            oyt = r2.r3y2;
                        }//case 3.2
                        else if (r2.r3y1 <= r1.r1y1 && r2.r3y2 > r1.r1y1 && r1.r1y2 <= r2.r3y2) {
                            overlaps = true;
                            oxl = r2.r3x1;
                            oyb = r1.r1y1;
                            oxr = r2.r3x2;
                            oyt = r1.r1y2;
                        } // Case 3.3: r2.y2 falls between r1.y1 and r1.y2
                        else if (r2.r3y1 <= r1.r1y2 && r2.r3y2 > r1.r1y2 && r1.r1y1 < r2.r3y1) {
                            overlaps = true;
                            oxl = r2.r3x1;
                            oyb = r2.r3y1;
                            oxr = r2.r3x2;
                            oyt = r1.r1y2;
                        }//case 3.4
                        else if (r1.r1y1 < r2.r3y1 && r1.r1y2 > r2.r3y1) {
                            overlaps = true;
                            oxl = r2.r3x1;
                            oyb = r2.r3y1;
                            oxr = r2.r3x2;
                            oyt = r2.r3y2;
                       }
                }
                if (overlaps) 
                {
            //            System.out.println("*******OverLAp Found***********");
            //            System.out.println("oxl +\",\"+ oxr +\" +\"\\t\"+oyb+\",\"+\",\"+oyt"+oxl +","+ oxr +"\t"+oyb+","+","+oyt);
                        
                        double midx = (oxl+oxr)/2;
                        double midy = (oyb+oyt)/2;
            //            System.out.println("Mid POints"+midx+","+midy);
                        int x = 1;
             //           System.out.println((midx+">="+cellRect.x1 +"&&"+ midx +"<="+cellRect.x2 +"&&"+ midy+">="+cellRect.y1 +"&&"+ midy+"<="+cellRect.y2));
                        if((midx == cellRect.x1) || (midx == cellRect.x2) || (midy == cellRect.y1) || (midy<cellRect.y2))
                        {
                            midx = midx+0.1; midy = midy+0.1;
                        }
                        if(midx > cellRect.x1 && midx < cellRect.x2 && midy > cellRect.y1 && midy < cellRect.y2)
                        {
                  //          System.out.println("*******MID POINT***********");
                            LongWritable key = new LongWritable(x);
                            String output = r1.r1x1+ "," +r2.r1y1+ "," +r2.r1x2+ "," +r2.r1y2+ "\t" +r2.r2x1+ "," +r2.r2y1+ "," +r2.r2x2+ "," +r2.r2y2+ "\t" +r2.r3x1+ "," +r2.r3y1+ "," +r2.r3x2+ "," +r2.r3y2+ "\t" +r1.r1x1+ "," +r1.r1y1+ "," +r1.r1x2+ "," +r1.r1y2;
                            Text Result = new Text(output);
                //            System.out.println(Result);
                            context.write(key, Result);   
                        }
                    }
                else{
             //            System.out.println("*******OverLAp NOT Found***********");
                }
            }
        }
    }
}