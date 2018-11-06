package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple6;

import java.io.Serializable;

/*
public class AvgSpeedFine extends Tuple6<Long, Long, String, Integer, Integer, Double> {
    public AvgSpeedFine() {
    }

    public void setTime1(Long time) {
        f0 = time;
    }

    public void setTime2(Long time) {
        f1 = time;
    }

    public void setVid(String vid) {
        f2 = vid;
    }

    public void setXway(int highway) {
        f3 = highway;
    }

    public void setDirection(int direction) {
        f4 = direction;
    }

    public void setAvgSpeed(double avgSpeed) {
        f5 = avgSpeed;
    }
}
*/

public class AvgSpeedFine implements Serializable {

    private Long time1;
    private Long time2;
    private String vid;
    private int xway;
    private int direction;
    private double avgSpeed;

    public AvgSpeedFine(PositionEvent first, PositionEvent last, double avgSpeed) {
        this.time1 = first.getTime();
        this.time2 = last.getTime();
        this.vid = first.getVid();
        this.xway = first.getXway();
        this.direction = first.getDirection();
        this.avgSpeed = avgSpeed;
    }

    public AvgSpeedFine() {

    }

    public void setTime1(Long time1) {
        this.time1 = time1;
    }

    public void setTime2(Long time2) {
        this.time2 = time2;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public void setXway(int xway) {
        this.xway = xway;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public void setAvgSpeed(double avgSpeed) {
        this.avgSpeed = avgSpeed;
    }

    public String toString() {
        // Returns the object attributes as a comma separated string
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(time1)).append(",");
        sb.append(Long.toString(time2)).append(",");
        sb.append(vid).append(",");
        sb.append(Integer.toString(xway)).append(",");
        sb.append(Integer.toString(direction)).append(",");
        sb.append(Double.toString(avgSpeed));

        return sb.toString();
    }
}

