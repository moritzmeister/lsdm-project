package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple7;

/**
 * ACCIDENT CLASS
 *
 * This class is a datatype used to generate the tuple
 * related to an accident. If an accident occurs a tuple
 * is generated, ready to be written.
 *
 * Output File: "accidents.csv"
 *
 * 1: Time1, in seconds identifying the time at which the position event was emitted. Using Long since in reality a data
 * stream is infinite. It comes from the first event.
 * 2: Time2, in seconds identifying the time at which the position event was emitted. Using Long since in reality a data
 * stream is infinite. It comes from the fourth event.
 * 3: VID, a string that identifies the vehicle.
 * 4: XWay, an integer identifying the highway from which the position report is emitted (0...L−1).
 * 5: Seg, an integer identifying the segment from which the position report is emitted (0...99).
 * 6: Dir, an integer identifying the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling.
 * 7: Pos, an integer identifying the horizontal position of the vehicle as the number of meters from the westernmost
 * point on the highway (i.e., Pos = x, 0...527999)
 */

public class Accident extends Tuple7<Long, Long, String, Integer, Integer, Integer, Integer> {

    public Accident() {
    }

    /*
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

    /*
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
