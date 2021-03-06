package Sequential;

import EMSpatialJoin.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleReducerSeqBC extends Reducer<LongWritable, ABJoinTuple, LongWritable, Text> {

    public void reduce(LongWritable key, Iterable<ABJoinTuple> values, Context context) throws IOException, InterruptedException {
        ArrayList<ABJoinTuple> lb = new ArrayList<>();
        ArrayList<ABJoinTuple> lc = new ArrayList<>();
     //   ArrayList<RectangleSeq> lc = new ArrayList<>();
     //   ArrayList<RectangleSeq> ld = new ArrayList<>();
       // int i, j;
        Configuration conf = context.getConfiguration();
        double cellWidth = conf.getDouble("cellWidth", 0.0);
            //System.out.println("cellWidth--:  "+cellWidth);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
            //System.out.println("cellsPerRow--:  "+cellsPerRow);
        int cellNumber =(int) key.get();
            //System.out.println("cellNumber:"+cellNumber);
        int cellRow = cellNumber/cellsPerRow;
            //System.out.println("cellRow--:  "+cellRow);
        int cellCol = cellNumber%cellsPerRow;
            //System.out.println("cellCol:  "+cellCol);
            //System.out.println("cellRect = (0,0,cellCol * cellWidth,cellRow * cellWidth,(cellCol+1) * cellWidth,(cellRow +1) * cellWidth)\n"
                           //        +"-->cellCol * cellWidth--:"+cellCol+ "*" +cellWidth+"="+cellCol * cellWidth 
                           //        +"\n-->cellRow * cellWidth--:"+cellRow+ "*" +cellWidth+"="+ cellRow * cellWidth 
                           //        +"\n-->(cellCol+1) * cellWidth--:"+ cellCol+"+" + 1 + "*"+ cellWidth+"="+(cellCol+1) * cellWidth 
                           //        +"\n-->(cellRow +1) * cellWidth)--:"+ cellRow+"+" + 1 + "*"+ cellWidth+"="+(cellRow +1) * cellWidth);
        RectangleSeq cellRect = new RectangleSeq(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
            
        
        System.out.println("Reducer got ");
        
        for (ABJoinTuple t : values) {
            ABJoinTuple newt = new ABJoinTuple(t);
            if(t.abType == 1){
                if (t.r2RelationIndex == 2) {
                    System.out.println("abType==1");
                    System.out.println("if (t.r2RelationIndex == 2)"+t.r2RelationIndex);
                lb.add(newt);
            }   else {
                }
            }
            else if(t.abType == 2){
                if (t.r1RelationIndex == 3) {
                System.out.println("abType==2");
                System.out.println("if (t.r1RelationIndex == 3)"+t.r1RelationIndex);
                lc.add(newt);
            }   else {
                }
            }
        }
         
        join(cellNumber,lb, lc, 2, cellRect,context);
     //   join(cellNumber,lb, lc, 2, cellRect, context);
     //   join(cellNumber,lc, ld, 3, cellRect, context);
        
    }

 /*   public void printArrayList(ArrayList<Rectangle> la) {
        for (int i = 0; i < la.size(); i++) {
          System.out.println(la.get(i).toString());
      }

    }  */

    public void join(int CellNumber,ArrayList<ABJoinTuple> lb, ArrayList<ABJoinTuple> lc, int joinType, RectangleSeq cellRect,
                                    Context context) throws IOException, InterruptedException {
        for (int i = 0; i < lb.size(); i++) {
            ABJoinTuple r1 = lb.get(i);
            for (int j = 0; j < lc.size(); j++) {
                boolean overlaps = false;
                ABJoinTuple r2 = lc.get(j);
                double oxl = 0, oxr = 0, oyb = 0, oyt = 0;
                
                //Case 1 : r2.x1 falls between r1.x1 and r1.x2
                if (r2.r1x1 <= r1.r2x1 && r2.r1x2 > r1.r2x1) {
               
                //    System.out.println("CASE 1:Main ");
                //    System.out.println("cond 1: if(r1.x1 <= r2.x1 && r1.x2 > r2.x1)-->"+r1.x1+" <="+ r2.x1+ "&&"+ r1.x2+" > "+r2.x1);
                    // Case 1
                    if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 > r2.r1y2) {
                    //    System.out.println("CASE 1: 1.1 ");
                    //    System.out.println( "cond 1.1: if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2)-->"+r1.y1+"<=" +r2.y1+" &&"+ r1.y2+" > "+r2.y1+" &&" +r2.y2+ ">" +r1.y2);
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                        System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                  //      System.out.println("oxl+oyb+oxr+oyt-->"+oxl+oyb+oxr+oyt);
                    } // Case 2 
                    else if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 <= r2.r1y2) {
                        System.out.println("CASE 1: 1.2 ");
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                        System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                    } // Case 3: 
                    else if (r2.r1y1 <= r1.r2y2 && r2.r1y2 > r1.r2y2 && r1.r2y1 < r1.r1y1) {
                        System.out.println("CASE 1: 1.3 ");
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    //    System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                    } //Case 4:
                    else if (r2.r1y2 < r1.r1y2 && r1.r2y1 < r2.r1y1) {
                        System.out.println("CASE 1: 1.4 ");
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
                    System.out.println("CASE 2: Main");
                        // Case 2a: r2.y1 falls between r1.y1 and r1.y2
                        if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 > r2.r1y2) {
                            System.out.println("CASE 2: 2.1 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r1.r2y1;
                            oxr = r1.r2x2;
                            oyt = r2.r1y2;
                            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                           // System.out.println("Did upto here...");
                        } else if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 <= r2.r1y2) {
                            System.out.println("CASE 2: 2.2 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r1.r2y1;
                            oxr = r1.r2x2;
                            oyt = r1.r2y2;
                            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                        else if (r2.r1y1 <= r1.r2y2 && r2.r1y2 > r1.r2y2 && r1.r2y1 < r2.r1y1) {
                            System.out.println("CASE 2: 2.3");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r2.r1y1;
                            oxr = r1.r2x2;
                            oyt = r1.r2y2;
                            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } else if (r1.r2y1 < r2.r1y1 && r1.r2y2 > r2.r1y1) {
                            System.out.println("CASE 2: 2.4 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r2.r1y1;
                            oxr = r1.r2x2;
                            oyt = r2.r1y2;
                            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        }
                } else if (r1.r2x1 < r2.r1x1 && r1.r2x2 > r2.r1x2) {
                    System.out.println("CASE 3: Main ");
                        if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 > r2.r1y2) {
                            System.out.println("CASE 3: 3.1 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r1.r2y1;
                            oxr = r2.r1x2;
                            oyt = r2.r1y2;
                            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } else if (r2.r1y1 <= r1.r2y1 && r2.r1y2 > r1.r2y1 && r1.r2y2 <= r2.r1y2) {
                            System.out.println("CASE 3: 3.2 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r1.r2y1;
                            oxr = r2.r1x2;
                            oyt = r1.r2y2;
                            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                        else if (r2.r1y1 <= r1.r2y2 && r2.r1y2 > r1.r2y2 && r1.r2y1 < r2.r1y1) {
                            System.out.println("CASE 3: 3.3");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r2.r1y1;
                            oxr = r2.r1x2;
                            oyt = r1.r2y2;
                            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        } else if (r1.r2y1 < r2.r1y1 && r1.r2y2 > r2.r1y1) {
                            System.out.println("CASE 3: 3.4 ");
                            overlaps = true;
                            oxl = r2.r1x1;
                            oyb = r2.r1y1;
                            oxr = r2.r1x2;
                            oyt = r2.r1y2;
                            System.out.println("oxl--"+oxl+"oyb--"+oyb+"oxr--"+oxr+"oyt--"+oyt);
                        }

                    }
               
                    if (overlaps) {
// output it to the reducer
                        // if mid point of the overlap region lies in the current cell area then compute the 
                        // join tuple otherwise forget
                      //  System.out.println("\n\nr1.x1+r1.y1+r1.x2+r1.y2+r2.x1+r2.y1+r2.x2+r2.y2-->"+ r1.x1+","+r1.y1+",\t"+r1.x2+","+r1.y2+",\n"+r2.x1+","+r2.y1+",\t"+r2.x2+","+r2.y2);
                        double midx = (oxl+oxr)/2;
                      //  System.out.println("\n\nmidx= oxl+oxr/2 \t oxl+\",\"+oxr/2+\",\"+midx-->"+oxl+","+oxr/2+","+midx);
                        double midy = (oyb+oyt)/2;
                     //   System.out.println("\n\nmidy = oyb+oyt/2 \t oyb+\",\"+oyt/2+\",\"+midy-->"+oyb+","+oyt/2+","+midy);
                        int x = 1;
                        if(midx>cellRect.x1 && midx <cellRect.x2 && midy>cellRect.y1 && midy<cellRect.y2){
                     //   System.out.println("\n\nif(midx>cellRect.x1 && midx <cellRect.x2 && midy>cellRect.y1 && midy<cellRect.y2)\n");
                      //  System.out.println("\n\nif--cellRect"+cellRect);
                    //    System.out.println("\n\nif--Cell Number " + CellNumber);
                    System.out.println("-------after if in ReducerBC------");
                            LongWritable key = new LongWritable(x);
                            String output = joinType + "," + r1 + "," + r2;
                          //  System.out.println("\n\nif output is-->"+"\t"+output);
                            Text Result = new Text(output);
                            context.write(key, Result);   
                        }
                        
                                          }
                    else{
                         System.out.println("-----------Reducer BC---No Overlap found-----------");
                    }
              }
        }
    }
}