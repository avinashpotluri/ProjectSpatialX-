package AllRepSpatialJoin;
/*
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleReducer extends Reducer<LongWritable, Rectangle, LongWritable, Text> 
{
    public void reduce(LongWritable key, Iterable<Rectangle> values, Context context) throws IOException, InterruptedException 
    {
        int i,j;
        Configuration conf = context.getConfiguration();
        
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);   // 5
     //   int cellsPerCol = conf.getInt("p1NumOfReducersPerCol",0);   // 5
     //   int totalCells = conf.getInt("p1NumOfReducers",0);  //25
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        Rectangle cellRect = new Rectangle(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
        int key1;
        for (Rectangle t : values) 
        {
           // Rectangle newt = new Rectangle(t);
            String line = t.toString();
            for(i= cellRow; i< cellsPerRow ; i++) 
            {
                for(j=cellCol; j< (cellsPerRow) ; j++) 
                {
                    key1= (i * cellsPerRow) + j;
                 //   System.out.println(cellNumber+"\t"+key1+"\t AB--------->"+line);
                    context.write(new LongWritable(key1), new Text(line));
                }
            }
        }
    }
}
*/

import Sequential.RectangleSeq;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RectangleReducer extends Reducer<LongWritable,Rectangle,LongWritable,Text> 
{
    public void reduce(LongWritable key, Iterable<Rectangle> value,Context context) throws IOException, InterruptedException 
    {
        ArrayList<Rectangle> la = new ArrayList<>();
        ArrayList<Rectangle> lb = new ArrayList<>();
        ArrayList<Rectangle> lc = new ArrayList<>();
        ArrayList<Rectangle> ld = new ArrayList<>();
        
        Configuration conf = context.getConfiguration();
        
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        Rectangle cellRect = new Rectangle(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
        for(Rectangle t:value)
        {
            Rectangle newt = new Rectangle(t);
            if (t.relationIndex == 1) {
                la.add(newt);
            } else if (t.relationIndex == 2) {
                lb.add(newt);
            } else if (t.relationIndex == 3) {
                lc.add(newt);
            } else if (t.relationIndex == 4) {
                ld.add(newt);
            } 
            else {
            }
        }
        join(cellNumber, cellRow, cellCol, cellsPerRow, la, lb, lc, ld, 1, 2, 3, cellRect,context);
    }
    public void join(int CellNumber, int cellRow, int cellCol, int cellsPerRow, ArrayList<Rectangle> la, ArrayList<Rectangle> lb, ArrayList<Rectangle> lc, ArrayList<Rectangle> ld, int joinType1, int joinType2, int joinType3, Rectangle cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        boolean overlaps;
        double x, y;
        int key1=0;
        System.out.println("la Size---->\t"+la.size());
        System.out.println("lb Size---->\t"+lb.size());
        System.out.println("lc Size---->\t"+lc.size());
        System.out.println("ld Size---->\t"+ld.size());
        for (int i = 0; i < la.size(); i++) 
        {
            Rectangle r1 = la.get(i);
            for (int j = 0; j < lb.size(); j++) 
            {
                Rectangle r2 = lb.get(j);
                overlaps = checkOverlap(r1,r2);
                if (overlaps) 
                {
                  //  System.out.println("-------------Overlap found b/n A and B-----------");
                    for (int k = 0; k < lc.size(); k++) 
                    {
                        Rectangle r3 = lc.get(k);
                        overlaps = checkOverlap(r2,r3);
                        if (overlaps) 
                        {
                            for (int l = 0; l < ld.size(); l++) 
                            {
                                Rectangle r4 = ld.get(l);
                                overlaps = checkOverlap(r3,r4);
                                if (overlaps) 
                                {
                    //                System.out.println("-------------Overlap found b/n A, B, C and D-----------");
                                    double xx=checkRightMostRectangle(r1,r2,r3,r4);
                                    double yy=checkLowerMostRectangle(r1,r2,r3,r4);
                                    String output = r1 +","+ r2 +","+ r3 + "," + r4 ;
                                    Text Result = new Text(output);
                                            
                                    key1= (cellRow * cellsPerRow) + cellCol;
                            //        System.out.println((cellRect.x1 +"< "+ xx) +" && "+  (cellRect.x2 +">"+  xx) +"&&"+  (cellRect.y1 +"< "+  yy) +"&& "+  (cellRect.y2 +"> "+  yy));    
                                    if((cellRect.x1 < xx) && (cellRect.x2 > xx) && (cellRect.y1 < yy) && (cellRect.y2 > yy))
                                    {
                                        context.write(new LongWritable(key1), Result);
                                    }
                                }
                                else 
                                {
                                    System.out.println(" Overlap not found b/n C and D-----------");
                                }
                            }
                        }
                        else
                        {
                            System.out.println(" Overlap not found b/n B and C-----------");
                        }
                    }
                }
                else
                {
                    System.out.println("-------------No Overlap found b/n A and B-----------");
                }
            }
        }
        
    }
    boolean checkOverlap(Rectangle r1, Rectangle r2) 
    {
        boolean overlaps=false;
                if (r1.x1 <= r2.x1 && r1.x2 > r2.x1) 
                {
                    if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                        overlaps = true;
                    } 
                    else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                        overlaps = true;
                    } 
                    else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 < r1.y1) {
                        overlaps = true;
                    } 
                    else if (r1.y2 < r2.y2 && r2.y1 < r1.y1) {
                        overlaps = true;
                    } 
                } 
                //Case 2
                else if (r1.x1 <= r2.x2 && r1.x2 > r2.x2) 
                {
                        if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) 
                        {
                            overlaps = true;
                        } else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                            overlaps = true;
                        } else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 < r1.y1) 
                        {
                            overlaps = true;
                        } else if (r2.y1 < r1.y1 && r2.y2 > r1.y1) 
                        {
                            overlaps = true;
                        }
                }
                // Case 3:
                else if (r2.x1 < r1.x1 && r2.x2 > r1.x2) 
                {
                        if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) 
                        {
                            overlaps = true;
                        } else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) 
                        {
                            overlaps = true;
                        } 
                        else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 < r1.y1) 
                        {
                            overlaps = true;
                        } else if (r2.y1 < r1.y1 && r2.y2 > r1.y1) 
                        {
                            overlaps = true;
                        }
                }
                return overlaps;
    }
    double checkRightMostRectangle(Rectangle r1, Rectangle r2, Rectangle r3, Rectangle r4) 
    {
   //     System.out.println(r1.toString() +"\t"+ r2.toString()+"\t"+ r3.toString()+"\t"+r4.toString());
        double x=0;
        double p=(r1.x2 > r2.x2)?r1.x2:r2.x2;
        double q=(p > r3.x2)?p:r3.x2;
        double r=(q > r4.x2)?q:r4.x2;
    //    System.out.println("Largest X  \t"+r);
        if(r==r1.x2) {
            x=r1.x2;
        }
        else if(r==r2.x2) {
            x=r2.x2;
        }
        else if(r==r3.x2) {
            x=r3.x2;
        }
        else if(r==r4.x2) {
            x=r4.x2;
        }
  //      System.out.println(x);
        return x;   // It will Return Right Most rectangle's  x1 point
    }
    double checkLowerMostRectangle(Rectangle r1, Rectangle r2, Rectangle r3, Rectangle r4) 
    {
        double y=0;
        double p=(r1.y2 > r2.y2)?r1.y2:r2.y2;
        double q=(p > r3.y2)?p:r3.y2;
        double r=(q > r4.y2)?q:r4.y2;
  //      System.out.println("Largest Y  \t"+r);
        if(r==r1.y2) {
            y=r1.y2;
        }
        else if(r==r2.y2) {
            y=r2.y2;
        }
        else if(r==r3.y2) {
            y=r3.y2;
        }
        else if(r==r4.y2) {
            y=r4.y2;
        }   
        return y;   // It will Return Upper Most rectangle's starting y1 point
    }
}