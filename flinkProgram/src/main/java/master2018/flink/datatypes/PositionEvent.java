package master2018.flink.datatypes;

/**
 *  POSITIONEVENT CLASS
 *
 *  This class is a datatype used to generate the tuple
 *  related to a new incoming event. These tuples are
 *  the one coming from the input source and that will be
 *  used for processing.
 *
 *  1. time             : Timestamp of the event
 *                        Long because Flink measures time in milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
 *  2. vid              : Vehicle ID
 *                        Long because Flink measures time in milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
 *  3. speed            : Speed
 *                        Speed of the vehicle, it is int because it is bounded.
 *  4. x_way            : Highway ID
 *                        The ID identifying the highway a car is on, it is int because it is bounded.
 *  5. lane             : Lane
 *                        The ID identifying the lane a car is on in the highway, it is int because it is bounded.
 *  6. direction        : Direction ID
 *                        0 or 1, indicating the direction, east or west. Int because there are only two possible values.
 *  7. segment          : Segment Number
 *                        The ID identifying the segment of a highway a car is on, it is int because it is bounded.
 *  8. position         : Position Number
 *                        Position in meters from the western most point of the highway, int because it is bounded.
 *  9. hasWatermarkTime : Boolean hasWatermark
 *  10. watermarkTime   : Watermark
 */
public class PositionEvent {

    /**
     *  Variable Allocation
     */
    private Long time;
    private String vid;
    private int speed;
    private int xway;
    private int lane;
    private int direction;
    private int segment;
    private int position;

    /**
     *  WaterMarks Allocation
     */
    private boolean hasWatermarkTime;
    private Long watermarkTime;

    /**
      * Class Constructor
      *
      * Constructs a PositionEvent event object from two PositionEvents, the first and the fourth event
      * of a series that a car hasn't moved.
      *
      * @param line contains the string passed as arguments from terminal
      */
    public PositionEvent(String[] line){
        String[] args = line;

        this.time = Long.parseLong(args[0]);
        this.vid = args[1];
        this.speed = Integer.parseInt(args[2]);
        this.xway = Integer.parseInt(args[3]);
        this.lane = Integer.parseInt(args[4]);
        this.direction = Integer.parseInt(args[5]);
        this.segment = Integer.parseInt(args[6]);
        this.position = Integer.parseInt(args[7]);
        this.hasWatermarkTime = false;
        this.watermarkTime = null;
    }

    /**
     *  Getter and Setters
     */
    public boolean hasWatermarkTime() {
        return this.hasWatermarkTime;
    }

    public void setWatermark() {
        this.hasWatermarkTime = true;
        this.watermarkTime = this.time;
    }

    public Long getWatermarkTime() {
        return (this.watermarkTime);
    }

    public Long getTime() {
        // Flink works in milliseconds
        return (this.time);
    }

    public String getVid() {
        return this.vid;
    }

    public int getSpeed() {
        return this.speed;
    }

    public int getXway() {
        return this.xway;
    }

    public int getLane() {
        return this.lane;
    }

    public int getDirection() {
        return this.direction;
    }

    public int getSegment() {
        return this.segment;
    }

    public int getPosition() {
        return this.position;
    }

    /**
     *  Object Conversion to String - Returns the object attributes as a comma separated string:
     *
     *  @return String <Time,VID,Speed,Highway,Lane,Direction,Segment,Position,HasWatermarkBoolean,Watermark>
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(time)).append(",");
        sb.append(vid).append(",");
        sb.append(Integer.toString(speed)).append(",");
        sb.append(Integer.toString(xway)).append(",");
        sb.append(Integer.toString(lane)).append(",");
        sb.append(Integer.toString(direction)).append(",");
        sb.append(Integer.toString(segment)).append(",");
        sb.append(Integer.toString(position)).append(", WatermarkTime: ");
        sb.append(hasWatermarkTime).append(",");
        sb.append(watermarkTime);

        return sb.toString();
    }
}
