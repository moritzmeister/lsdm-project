package master2018.flink.datatypes;

/*
   POSITIONEVENT CLASS

   This class is a datatype used to generate the tuple
   related to a new incoming event. These tuples are
   the one coming from the input source and that will be
   used for processing.
*/
public class PositionEvent {

    /* Variable Allocation */
    private Long time;
    private String vid;
    private int speed;
    private int xway;
    private int lane;
    private int direction;
    private int segment;
    private int position;

    /* WaterMarks Allocation */
    private boolean hasWatermarkTime;
    private Long watermarkTime;

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
       8. hasWatermarkTime = Boolean hasWatermark
       9. watermarkTime    = Watermark
    */
    public PositionEvent(String[] line){
        /* Constructs from terminal arguments */
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

    /* Getter and Setters */
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

    /*
       Object Conversion to String - Returns the object attributes as a comma separated string:

       String: <Time,VID,Speed,Highway,Lane,Direction,Segment,Position,HasWatermarkBoolean,Watermark>
    */
    public String toString() {
        /* Returns the object attributes as a comma separated string */
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
