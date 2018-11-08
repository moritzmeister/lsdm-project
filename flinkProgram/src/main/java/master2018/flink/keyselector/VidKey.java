package master2018.flink.keyselector;

import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * VIDKEY CLASS
 *
 * This class is a structure that can be used in the keyBy
 * to group the item from the following IDs:
 *
 * 1: VID, a string that identifies the vehicle.
 * 2: XWay, an integer identifying the highway from which the position report is emitted (0...L−1).
 * 3: Dir, an integer identifying the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling.
 */
public class VidKey implements KeySelector<PositionEvent, Tuple3<String, Integer, Integer>> {

    @Override
    public Tuple3 getKey(PositionEvent data) {
        Tuple3 key = new Tuple3();
        key.f0 = data.getVid();
        key.f1 = data.getXway();
        key.f2 = data.getDirection();
        return key;
    }
}