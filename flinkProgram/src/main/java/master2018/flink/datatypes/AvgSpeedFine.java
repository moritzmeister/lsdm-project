package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * AVGSPEEDFINE CLASS
 *
 * This class is a datatype used to generate the tuple
 * related to fines emitted if the car runs faster than 60 mph between the highway segments 52 and 56.
 *
 * Output File: "avgspeedfines.csv"
 *
 * 0: Time, in seconds identifying the first time at which the position event was emitted when checking the average
 * speed in the said highway part. Using Long since in reality a data stream is infinite.
 * 1: Time, in seconds identifying the last time at which the position event was emitted when checking the average
 * speed in the said highway part.
 * 2: VID, a string that identifies the vehicle.
 * 3: XWay, an integer identifying the highway from which the position report is emitted (0...L−1).
 * 4: Dir, an integer identifying the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling.
 * 5: Average Speed, an integer that represents the speed mph (miles per hour) of the vehicle (0-100).
 */

public class AvgSpeedFine extends Tuple6<Long, Long, String, Integer, Integer, Double> {

    public AvgSpeedFine() {
    }

    /*
     * Constructs a PositionEvent event object from two PositionEvents, the first and the last event of the car being
     * driven between segments 52 and 56 (both included) in both directions.
     *
     * @param firstElement First PositionEvent of the car that will be fined.
     * @param lastElement Last PositionEvent of the car that will be fined.
     * @param key A combination of VID, Highway and direction that groups the car PositionEvents.
     */
    public AvgSpeedFine(PositionEvent firstElement, PositionEvent lastElement,
                        Tuple key, double avgSpeed) {
        this.f0 = firstElement.getTime();
        this.f1 = lastElement.getTime();
        this.f2 = key.getField(0);
        this.f3 = key.getField(1);
        this.f4 = key.getField(2);
        this.f5 = avgSpeed;
    }

    /*
     * Returns the object attributes as a comma separated string.
     *
     * @return string <Time1,Time2,VID,XWay,Dir,AvgSpeed>
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(f0)).append(",");
        sb.append(Long.toString(f1)).append(",");
        sb.append(f2).append(",");
        sb.append(Integer.toString(f3)).append(",");
        sb.append(Integer.toString(f4)).append(",");
        sb.append(Double.toString(f5));

        return sb.toString();
    }
}

