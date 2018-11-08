package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple8;

/**
 * PositionEvent class is a custom datatype extending the Flink Tuple.
 * The instances are created in the source function from the lines of the input file.
 * These tuples are the starting point for all further transformations.
 * Note: The timestamp is saved in seconds, but Flink works in milliseconds, therefore we need to multiply by 1000 every
 * time we pass a timestamp to Flink.
 *
 * Fields:
 * 0: Time, in seconds identifying the time at which the position event was emitted. Using Long since in reality a data
 * stream is infinite
 * 1: VID, a string that identifies the vehicle.
 * 2: Speed, an integer that represents the speed mph (miles per hour) of the vehicle (0-100).
 * 3: XWay, an integer identifying the highway from which the position report is emitted (0...L−1).
 * 4: Lane, an integer identifying the lane of the highway from which the position report is emitted (0...4).
 * 5: Dir, an integer identifying the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling.
 * 6: Seg, an integer identifying the segment from which the position report is emitted (0...99).
 * 7: Pos, an integer identifying the horizontal position of the vehicle as the number of meters from the westernmost
 * point on the highway (i.e., Pos = x, 0...527999)
 */

public class PositionEvent extends Tuple8<Long, String, Integer, Integer, Integer, Integer, Integer, Integer> {

    public PositionEvent(){
    }

    public PositionEvent(String[] line){
        // Constructs from split string
        String[] args = line;

        this.f0 = Long.parseLong(args[0]);
        this.f1 = args[1];
        this.f2 = Integer.parseInt(args[2]);
        this.f3 = Integer.parseInt(args[3]);
        this.f4 = Integer.parseInt(args[4]);
        this.f5 = Integer.parseInt(args[5]);
        this.f6 = Integer.parseInt(args[6]);
        this.f7 = Integer.parseInt(args[7]);
    }

    public PositionEvent(String line){
        // Constructs from string line, read from a csv
        String[] args = line.split(",");

        this.f0 = Long.parseLong(args[0]);
        this.f1 = args[1];
        this.f2 = Integer.parseInt(args[2]);
        this.f3 = Integer.parseInt(args[3]);
        this.f4 = Integer.parseInt(args[4]);
        this.f5 = Integer.parseInt(args[5]);
        this.f6 = Integer.parseInt(args[6]);
        this.f7 = Integer.parseInt(args[7]);
    }

    // We are using getters in order to increase readability in the code
    public Long getTime() {
        return this.f0;
    }

    public String getVid() {
        return this.f1;
    }

    public int getSpeed() {
        return this.f2;
    }

    public int getXway() {
        return this.f3;
    }

    public int getLane() {
        return this.f4;
    }

    public int getDirection() {
        return this.f5;
    }

    public int getSegment() {
        return this.f6;
    }

    public int getPosition() {
        return this.f7;
    }


    // Object Conversion to String - Returns the object attributes as a comma separated string:
    // <Time,VID,Speed,Highway,Lane,Direction,Segment,Position>
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(f0)).append(",");
        sb.append(f1).append(",");
        sb.append(Integer.toString(f2)).append(",");
        sb.append(Integer.toString(f3)).append(",");
        sb.append(Integer.toString(f4)).append(",");
        sb.append(Integer.toString(f5)).append(",");
        sb.append(Integer.toString(f6)).append(",");
        sb.append(Integer.toString(f7));

        return sb.toString();
    }
}

