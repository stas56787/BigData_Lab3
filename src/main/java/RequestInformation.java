import java.io.Serializable;

public class RequestInformation implements Serializable {
    private long id;
    private long count;
    private long byteSum;
    private float byteAverage;

    public RequestInformation(String input) {
        String[] inputs = input.split(" ");
        this.id = Long.parseLong(inputs[0].substring(2));
        if (inputs[9].equals("-")) {
            this.byteSum = 0;
        }
        else {
            this.byteSum = Long.parseLong(inputs[9]);
        }
        count = 1;
        this.byteAverage = this.byteSum;
    }

    @Override
    public String toString() {
        return id + " " + byteAverage + " " + byteSum;
    }

    public RequestInformation() {
        this.byteAverage = 0;
        byteSum = 0;
    }

    public void addToSum(long byteCount) {
        byteSum += byteCount;
        count++;
        byteAverage = (float) byteCount / count;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getByteSum() {
        return byteSum;
    }

    public void setByteSum(long byteSum) {
        this.byteSum = byteSum;
    }

    public float getByteAverage() {
        return byteAverage;
    }

    public void setByteAverage(float byteAverage) {
        this.byteAverage = byteAverage;
    }
}
