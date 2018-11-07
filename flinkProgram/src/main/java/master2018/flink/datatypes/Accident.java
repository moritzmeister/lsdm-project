package master2018.flink.datatypes;

/**
 *  ACCIDENT CLASS
 *
 *  This class is a datatype used to generate the tuple
 *  related to an accident. If an accident occurs a tuple
 *  is generated, ready to be written.
 *
 *  Output File: "accidents.csv"
 *
 *  1. time1     : Timestamp from the first event
 *                 Long because Flink measures time in milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
 *  2. time2     : Timestamp from the fourth event
 *                 Long because Flink measures time in milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
 *  3. vid       : Vehicle ID
 *                 String to make it work with any identifier.
 *  4. x_way     : Highway ID
 *                 The ID identifying the highway a car is on, it is int because it is bounded.
 *  5. segment   : Segment Number
 *                 The ID identifying the segment of a highway a car is on, it is int because it is bounded.
 *  6. direction : Direction ID
 *                 0 or 1, indicating the direction, east or west. Int because there are only two possible values.
 *  7. position  : Position Number
 *                 Position in meters from the western most point of the highway, int because it is bounded.
 */

public class Accident {

    /**
     * Variable Allocation
     */
    private Long time1;
    private Long time2;
    private String vid;
    private int x_way;
    private int segment;
    private int direction;
    private int position;

    /**
     * Class Constructor
     * Constructs a Accident event object from two PositionEvents, the first and the fourth event
     * of a series that a car hasn't moved.
     *
     * @param data1 first PositionEvent of the four events that make up an accident
     * @param data2 fourth PositionEvent of the four events that make up an accident
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

    /**
     * Object Conversion to String - Returns the object attributes as a comma separated string:
     *
     * @return String <Time1,Time2,VID,Highway,Segment,Direction,Position>
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
