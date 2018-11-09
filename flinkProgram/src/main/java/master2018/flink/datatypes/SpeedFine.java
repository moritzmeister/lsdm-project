package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * SPEEDFINE CLASS
 *
 * This class is a datatype used to generate the tuple
 * related to fines emitted if the car run faster then
 * 90 KM/h.
 *
 * Output File: "speedfines.csv"
 *
 * 0: Time, in seconds identifying the time at which the position event was emitted. Using Long since in reality a data
 * stream is infinite.
 * 1: VID, a string that identifies the vehicle.
 * 2: XWay, an integer identifying the highway from which the position report is emitted (0...L−1).
 * 3: Seg, an integer identifying the segment from which the position report is emitted (0...99).
 * 4: Dir, an integer identifying the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling.
 * 5: Speed, an integer that represents the speed mph (miles per hour) of the vehicle (0-100).
 */

public class SpeedFine extends Tuple6<Long, String, Integer, Integer, Integer, Integer> {

    public SpeedFine() {
    }

    /*
     * Constructs a SpeedFine event object from one  PositionEvent.
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

    /*
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