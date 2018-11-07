package master2018.flink.datatypes;

/*
   POSITION EVENT CLASS

   This class is a datatype used to generate the tuple
   related to a new incoming event. These tuples are
   the one coming from the input source and that will be
   used for processing.
*/

import org.apache.flink.api.java.tuple.Tuple8;

public class PositionEvent extends Tuple8<Long, String, Integer, Integer, Integer, Integer, Integer, Integer> {

    /*
       Class Constructor
       1. time             = Timestamp from the first event
       2. vid              = Vehicle ID
       3. speed            = Speed
       4. x_way            = Highway ID
       5. lane             = Lane
       5. direction        = Segment Number
       6. segment          = Direction ID
       7. position         = Position Number
    */

    public PositionEvent(){
    }

    public PositionEvent(String[] line){
        /* Constructs from terminal arguments */
        String[] args = line;

        this.f0 = Long.parseLong(args[0]);
        this.f1 = args[1];
        this.f2 = Integer.parseInt(args[2]);
        this.f3 = Integer.parseInt(args[3]);
        this.f4 = Integer.parseInt(args[4]);
        this.f5 = Integer.parseInt(args[5]);
        this.f6 = Integer.parseInt(args[6]);
        this.f7 = Integer.parseInt(args[7]);
    }

    public PositionEvent(String line){
        /* Constructs from terminal arguments */
        String[] args = line.split(",");

        this.f0 = Long.parseLong(args[0]);
        this.f1 = args[1];
        this.f2 = Integer.parseInt(args[2]);
        this.f3 = Integer.parseInt(args[3]);
        this.f4 = Integer.parseInt(args[4]);
        this.f5 = Integer.parseInt(args[5]);
        this.f6 = Integer.parseInt(args[6]);
        this.f7 = Integer.parseInt(args[7]);
    }

    public Long getTime() {
        // Flink works in milliseconds
        return this.f0;
    }

    public String getVid() {
        return this.f1;
    }

    public int getSpeed() {
        return this.f2;
    }

    public int getXway() {
        return this.f3;
    }

    public int getLane() {
        return this.f4;
    }

    public int getDirection() {
        return this.f5;
    }

    public int getSegment() {
        return this.f6;
    }

    public int getPosition() {
        return this.f7;
    }

    /**
     *  Object Conversion to String - Returns the object attributes as a comma separated string:
     *
     *  @return String <Time,VID,Speed,Highway,Lane,Direction,Segment,Position,HasWatermarkBoolean,Watermark>
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(f0)).append(",");
        sb.append(f1).append(",");
        sb.append(Integer.toString(f2)).append(",");
        sb.append(Integer.toString(f3)).append(",");
        sb.append(Integer.toString(f4)).append(",");
        sb.append(Integer.toString(f5)).append(",");
        sb.append(Integer.toString(f6)).append(",");
        sb.append(Integer.toString(f7));

        return sb.toString();
    }
}

