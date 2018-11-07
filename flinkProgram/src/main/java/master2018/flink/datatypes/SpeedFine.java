package master2018.flink.datatypes;

/*
   SPEEDFINE CLASS

   This class is a datatype used to generate the tuple
   related to fines emitted if the car run faster then
   90 KM/h.

   Output File: "speedfines.csv"
*/
public class SpeedFine {

    /* Variable Allocation */
    private Long time;
    private String vid;
    private int speed;
    private int xway;
    private int lane;
    private int direction;

    /*
       Class Constructor
       1. time      = Timestamp for the event
       2. vid       = Vehicle ID
       3. speed     = Speed
       4. x_way     = Highway ID
       5. lane      = Highway's Lane
       6. direction = Direction ID
    */
    public SpeedFine(PositionEvent positionEvent) {
        this.time = positionEvent.getTime();
        this.vid = positionEvent.getVid();
        this.speed = positionEvent.getSpeed();
        this.xway = positionEvent.getXway();
        this.lane = positionEvent.getLane();
        this.direction = positionEvent.getDirection();
    }

    /*
       Object Conversion to String - Returns the object attributes as a comma separated string:

       String: <Time,VID,Speed,Highway,Lane,Direction>
    */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(time)).append(",");
        sb.append(vid).append(",");
        sb.append(Integer.toString(speed)).append(",");
        sb.append(Integer.toString(xway)).append(",");
        sb.append(Integer.toString(lane)).append(",");
        sb.append(Integer.toString(direction));

        return sb.toString();
    }
}
