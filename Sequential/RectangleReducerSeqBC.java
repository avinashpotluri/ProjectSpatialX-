package Sequential;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleReducerSeqBC extends Reducer<LongWritable, ABJoinTuple, LongWritable, Text> 
{
    public void reduce(LongWritable key, Iterable<ABJoinTuple> values, Context context) throws IOException, InterruptedException 
    {
        ArrayList<ABJoinTuple> lb = new ArrayList<>();
        ArrayList<ABJoinTuple> lc = new ArrayList<>();
        Configuration conf = context.getConfiguration();
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        RectangleSeq cellRect = new RectangleSeq(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
       // System.out.println("Reducer got ");
        
        for (ABJoinTuple t : values) 
        {
            ABJoinTuple newt = new ABJoinTuple(t);
        //    System.out.println("ReducerBC----->>"+"\t"+ newt);
         //   System.out.println("frm mapper AB in RED BC__________________________________________________"+"\t"+newt );
        //    System.out.println(t.r1RelationIndex+","+t.r2RelationIndex );
            if(t.abType == 1)
            {
                if (t.r2RelationIndex == 2) 
                {
        //            System.out.println("frm mapper AB in RED BC^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"+"\t"+newt );
                    lb.add(newt);
                }   
            }
            else if(t.abType == 2)
            {
                if (t.r1RelationIndex == 3) 
                {
        //            System.out.println("frm mapper AB in RED BC^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"+"\t"+newt );
                    lc.add(newt);
                }   
            }
            else {
            }
        }
        join(cellNumber,lb, lc, 2, cellRect,context);
    }

    public void join(int CellNumber,ArrayList<ABJoinTuple> lb, ArrayList<ABJoinTuple> lc, int joinType, RectangleSeq cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        for (int i = 0; i < lb.size(); i++) 
        {
            ABJoinTuple r1 = lb.get(i);
            for (int j = 0; j < lc.size(); j++) 
            {
                boolean overlaps = false;
                ABJoinTuple r2 = lc.get(j);
                double oxl = 0, oxr = 0, oyb = 0, oyt = 0;
                //Case 1 : r2.x1 falls between r1.x1 and r1.x2
                if (r2.r1x1 <= r1.r2x1 && r2.r1x2 > r1.r2x1) 
                {
               
                //    System.out.println("CASE 1:Main ");
                //    System.out.println("cond 1: if (r1.x1 <= r2.x1 && r1.x2 > r2.x1)-->"+r1.x1+" <="+ r2.x1+ "&&"+ r1.x2+" > "+r2.x1);
                    // Case 1
                    if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 > r2.r1y2) {
                    //    System.out.println("CASE 1: 1.1 ");
                    //    System.out.println( "cond 1.1: if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2)-->"+r1.y1+"<=" +r2.y1+" &&"+ r1.y2+" > "+r2.y1+" &&" +r2.y2+ ">" +r1.y2);
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                 //       System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                  //      System.out.println("oxl+oyb+oxr+oyt-->"+oxl+oyb+oxr+oyt);
                    } // Case 2 
                    else if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 <= r2.r1y2) {
                 //       System.out.println("CASE 1: 1.2 ");
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                 //       System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                    } // Case 3: 
                    else if (r2.r1y1 <= r1.r2y2 && r2.r1y2 > r1.r2y2 && r1.r2y1 < r1.r1y1) {
                //        System.out.println("CASE 1: 1.3 ");
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    //    System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                    } //Case 4:
                    else if (r2.r1y2 < r1.r1y2 && r1.r2y1 < r2.r1y1) {
                //        System.out.println("CASE 1: 1.4 ");
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                     //   System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                    } // Case 2: r2.x1 is less than r1.x1
                      //  r2.x2 is between r1.x1 and r1.x2
                }    
                
                else if (r2.r1x1 <= r1.r2x2 && r2.r1x2 > r1.r2x2) {
                //    System.out.println("CASE 2: Main");
                        // Case 2a: r2.y1 falls between r1.y1 and r1.y2
                        if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 > r2.r1y2) {
                //            System.out.println("CASE 2: 2.1 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r1.r2y1;
                            oxr = r1.r2x2;
                            oyt = r2.r1y2;
                 //           System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                           // System.out.println("Did upto here...");
                        } else if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 <= r2.r1y2) {
                 //           System.out.println("CASE 2: 2.2 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r1.r2y1;
                            oxr = r1.r2x2;
                            oyt = r1.r2y2;
                //            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                        else if (r2.r1y1 <= r1.r2y2 && r2.r1y2 > r1.r2y2 && r1.r2y1 < r2.r1y1) {
                 //           System.out.println("CASE 2: 2.3");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r2.r1y1;
                            oxr = r1.r2x2;
                            oyt = r1.r2y2;
                //            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } else if (r1.r2y1 < r2.r1y1 && r1.r2y2 > r2.r1y1) {
                 //           System.out.println("CASE 2: 2.4 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r2.r1y1;
                            oxr = r1.r2x2;
                            oyt = r2.r1y2;
                 //           System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        }
                } else if (r1.r2x1 < r2.r1x1 && r1.r2x2 > r2.r1x2) {
                 //   System.out.println("CASE 3: Main ");
                        if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 > r2.r1y2) {
                //            System.out.println("CASE 3: 3.1 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r1.r2y1;
                            oxr = r2.r1x2;
                            oyt = r2.r1y2;
                    //        System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } else if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 <= r2.r1y2) {
                   //         System.out.println("CASE 3: 3.2 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r1.r2y1;
                            oxr = r2.r1x2;
                            oyt = r1.r2y2;
                    //        System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                        else if (r2.r1y1 <= r1.r2y2 && r2.r1y2 > r1.r2y2 && r1.r2y1 < r2.r1y1) {
                    //        System.out.println("CASE 3: 3.3");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r2.r1y1;
                            oxr = r2.r1x2;
                            oyt = r1.r2y2;
                    //        System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } else if (r1.r2y1 < r2.r1y1 && r1.r2y2 > r2.r1y1) {
                   //         System.out.println("CASE 3: 3.4 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r2.r1y1;
                            oxr = r2.r1x2;
                            oyt = r2.r1y2;
                    //        System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        }

                    }
               
                    if (overlaps) {
// output it to the reducer
                        // if mid point of the overlap region lies in the current cell area then compute the 
                        // join tuple otherwise forget
                      //  System.out.println("\n\nr1.x1+r1.y1+r1.x2+r1.y2+r2.x1+r2.y1+r2.x2+r2.y2-->"+ r1.x1+","+r1.y1+",\t"+r1.x2+","+r1.y2+",\n"+r2.x1+","+r2.y1+",\t"+r2.x2+","+r2.y2);
                        double midx = (oxl+oxr)/2;
                        double midy = (oyb+oyt)/2;
                        int x = 1;
                        if((midx == cellRect.x1) || (midx == cellRect.x2) || (midy == cellRect.y1) || (midy<cellRect.y2))
                        {
                            midx = midx+0.1; midy = midy+0.1;
                        }
                        
                        if(midx>cellRect.x1 && midx <cellRect.x2 && midy>cellRect.y1 && midy<cellRect.y2)
                        {
                            LongWritable key = new LongWritable(x);
                            String output = joinType + "," + r1 + "," + r2;
                            Text Result = new Text(output);
                            context.write(key, Result);   
                        }
                        
                    }
                    else{
                  //       System.out.println("-----------Reducer BC---No Overlap found-----------");
                    }
            }
        }
    }
}