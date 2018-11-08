package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;

import javax.swing.text.Position;

public class AvgSpeedFine extends Tuple6<Long, Long, String, Integer, Integer, Double> {

    public AvgSpeedFine() {
    }

    public AvgSpeedFine(PositionEvent firstElement, PositionEvent lastElement,
                        Tuple key, double avgSpeed) {
        this.f0 = firstElement.getTime();
        this.f1 = lastElement.getTime();
        this.f2 = key.getField(0);
        this.f3 = key.getField(1);
        this.f4 = key.getField(2);
        this.f5 = avgSpeed;
    }

    public void setTime1(Long time1) {
        this.f0 = time1;
    }

    public void setTime2(Long time2) {
        this.f1 = time2;
    }

    public void setVid(String vid) {
        this.f2 = vid;
    }

    public void setXway(int xway) {
        this.f3 = xway;
    }

    public void setDirection(int direction) {
        this.f4 = direction;
    }

    public void setAvgSpeed(double avgSpeed) {
        this.f5 = avgSpeed;
    }

    public String toString() {
        // Returns the object attributes as a comma separated string
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

