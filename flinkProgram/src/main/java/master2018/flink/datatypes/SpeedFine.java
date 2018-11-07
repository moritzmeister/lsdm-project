package master2018.flink.datatypes;

/**
 *  SPEEDFINE CLASS
 *
 *  This class is a datatype used to generate the tuple
 *  related to fines emitted if the car run faster then
 *  90 KM/h.
 *
 *  Output File: "speedfines.csv"
 *
 *  1. time      : Timestamp of the event
 *                 Long because Flink measures time in milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
 *  2. vid       : Vehicle ID
 *                 Long because Flink measures time in milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
 *  3. speed     : Speed
 *                 Speed of the vehicle, it is int because it is bounded.
 *  4. x_way     : Highway ID
 *                 The ID identifying the highway a car is on, it is int because it is bounded.
 *  5. lane      : Lane
 *                 The ID identifying the lane a car is on in the highway, it is int because it is bounded.
 *  6. direction : Direction ID
 *                 0 or 1, indicating the direction, east or west. Int because there are only two possible values.
 */
public class SpeedFine {

    /**
     * Variable Allocation
     */
    private Long time;
    private String vid;
    private int speed;
    private int xway;
    private int segment;
    private int direction;

    /**
     * Constructs a PositionEvent event object from two PositionEvents, the first and the fourth event
     * of a series that a car hasn't moved.
     *
     * @param positionEvent PositionEvent of the car that will be fined
    */
    public SpeedFine(PositionEvent positionEvent) {
        this.time = positionEvent.getTime();
        this.vid = positionEvent.getVid();
        this.speed = positionEvent.getSpeed();
        this.xway = positionEvent.getXway();
        this.segment = positionEvent.getSegment();
        this.direction = positionEvent.getDirection();
    }


    /**
     * Object Conversion to String - Returns the object attributes as a comma separated string:
     *
     * @return String: <Time,VID,Highway,Segment,Direction,Speed>
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(time)).append(",");
        sb.append(vid).append(",");
        sb.append(Integer.toString(xway)).append(",");
        sb.append(Integer.toString(segment)).append(",");
        sb.append(Integer.toString(direction)).append(",");
        sb.append(Integer.toString(speed));

        return sb.toString();
    }
}