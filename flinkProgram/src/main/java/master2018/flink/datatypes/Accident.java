package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple7;

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

public class Accident extends Tuple7<Long, Long, String, Integer, Integer, Integer, Integer> {

    public Accident() {
    }

    /**
     * Class Constructor
     * Constructs a Accident event object from two PositionEvents, the first and the fourth event
     * of a series that a car hasn't moved.
     *
     * @param data1 first PositionEvent of the four events that make up an accident
     * @param data2 fourth PositionEvent of the four events that make up an accident
     */
    public Accident(PositionEvent data1, PositionEvent data2) {
        this.f0 = data1.getTime();
        this.f1 = data2.getTime();
        this.f2 = data2.getVid();
        this.f3 = data2.getXway();
        this.f4 = data2.getSegment();
        this.f5 = data2.getDirection();
        this.f6 = data2.getPosition();
    }

    /**
     * Object Conversion to String - Returns the object attributes as a comma separated string:
     *
     * @return String <Time1,Time2,VID,Highway,Segment,Direction,Position>
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(f0)).append(",");
        sb.append(Long.toString(f1)).append(",");
        sb.append(f2).append(",");
        sb.append(Integer.toString(f3)).append(",");
        sb.append(Integer.toString(f4)).append(",");
        sb.append(Integer.toString(f5)).append(",");
        sb.append(Integer.toString(f6));

        return sb.toString();
    }
}
