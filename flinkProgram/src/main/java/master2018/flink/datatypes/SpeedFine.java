package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple6;

/*
   SPEEDFINE CLASS

   This class is a datatype used to generate the tuple
   related to fines emitted if the car run faster then
   90 KM/h.

   Output File: "speedfines.csv"
*/
public class SpeedFine extends Tuple6<Long, String, Integer, Integer, Integer, Integer> {

    public SpeedFine() {
    }

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
        this.f0 = positionEvent.getTime();
        this.f1 = positionEvent.getVid();
        this.f2 = positionEvent.getXway();
        this.f3 = positionEvent.getSegment();
        this.f4 = positionEvent.getDirection();
        this.f5 = positionEvent.getSpeed();
    }

    /*
       Object Conversion to String - Returns the object attributes as a comma separated string:

       String: <Time,VID,Highway,Segment,Direction,Speed>
    */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(f0)).append(",");
        sb.append(f1).append(",");
        sb.append(Integer.toString(f2)).append(",");
        sb.append(Integer.toString(f3)).append(",");
        sb.append(Integer.toString(f4)).append(",");
        sb.append(Integer.toString(f5));

        return sb.toString();
    }
}