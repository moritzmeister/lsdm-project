package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple6;

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

public class SpeedFine extends Tuple6<Long, String, Integer, Integer, Integer, Integer> {

    public SpeedFine() {
    }

    /**
     * Constructs a PositionEvent event object from two PositionEvents, the first and the fourth event
     * of a series that a car hasn't moved.
     *
     * @param positionEvent PositionEvent of the car that will be fined
    */
    public SpeedFine(PositionEvent positionEvent) {
        this.f0 = positionEvent.getTime();
        this.f1 = positionEvent.getVid();
        this.f2 = positionEvent.getXway();
        this.f3 = positionEvent.getSegment();
        this.f4 = positionEvent.getDirection();
        this.f5 = positionEvent.getSpeed();
    }

    /**
     * Object Conversion to String - Returns the object attributes as a comma separated string:
     *
     * @return String: <Time,VID,Highway,Segment,Direction,Speed>
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