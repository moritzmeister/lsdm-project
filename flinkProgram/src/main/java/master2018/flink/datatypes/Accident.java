package master2018.flink.datatypes;

/*
   ACCIDENT CLASS

   This class is a datatype used to generate the tuple
   related to an accident. If an accident occurs a tuple
   is generated, ready to be written.

   Output File: "accidents.csv"
*/
public class Accident {

    /* Variable Allocation */
    private Long time1;
    private Long time2;
    private String vid;
    private int x_way;
    private int segment;
    private int direction;
    private int position;

    /*
       Class Constructor
       1. time1     = Timestamp from the first event
       2. time2     = Timestamp from the fourth event
       3. vid       = Vehicle ID
       4. x_way     = Highway ID
       5. segment   = Segment Number
       6. direction = Direction ID
       7. position  = Position Number
    */
    public Accident(PositionEvent data1, PositionEvent data2) {
        this.time1 = data1.getTime();
        this.time2 = data2.getTime();
        this.vid = data2.getVid();
        this.x_way = data2.getXway();
        this.segment = data2.getSegment();
        this.direction = data2.getDirection();
        this.position = data2.getPosition();
    }

    /*
       Object Conversion to String - Returns the object attributes as a comma separated string:

       String: <Time1,Time2,VID,Highway,Segment,Direction,Position>
    */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(time1)).append(",");
        sb.append(Long.toString(time2)).append(",");
        sb.append(vid).append(",");
        sb.append(Integer.toString(x_way)).append(",");
        sb.append(Integer.toString(segment)).append(",");
        sb.append(Integer.toString(direction)).append(",");
        sb.append(Integer.toString(position));

        return sb.toString();
    }
}
