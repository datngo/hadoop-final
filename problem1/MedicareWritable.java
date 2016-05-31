package finalproject.problem1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MedicareWritable implements WritableComparable<MedicareWritable> {

    private Text codeTreatment = new Text();
    private Text codeInsurance = new Text();
    private Text nameInsurance = new Text();
    private Text addressInsurance = new Text();
    private Text cityInsurance = new Text();
    private Text stateInsurance = new Text();
    private Text zipInsurance = new Text();
    private Text geolocation = new Text();
    private Integer numberRequest = new Integer(1);// need to confirm
    private DoubleWritable billRequest = new DoubleWritable();
    private DoubleWritable billActual = new DoubleWritable();

    // Default Constructor
    public MedicareWritable() {
        this.codeTreatment = new Text();
        this.billRequest = new DoubleWritable();
    }

    // Custom Constructor
    public MedicareWritable(Text codeTreatment, DoubleWritable billRequest) {
        this.codeTreatment = codeTreatment;
        this.billRequest = billRequest;
    }

    // Setter method to set the values of WebLogWritable object
    public void set(Text codeTreatment) {
        this.codeTreatment = codeTreatment;
    }

    // Setter method to set the values of WebLogWritable object
    public void set(Text codeTreatment, DoubleWritable billRequest) {
        this.codeTreatment = codeTreatment;
        this.billRequest = billRequest;
    }

    // to get IP address from WebLog Record
    public Text getCodeTreatment() {
        return codeTreatment;
    }

    @Override
    // overriding default readFields method.
    // It de-serializes the byte stream data
    public void readFields(DataInput in) throws IOException {
        codeTreatment.readFields(in);
        billRequest.readFields(in);
    }

    @Override
    // It serializes object data into byte stream data
    public void write(DataOutput out) throws IOException {
        codeTreatment.write(out);
        billRequest.write(out);
    }

    @Override
    public int compareTo(MedicareWritable o) {
        // if (ipaddress.compareTo(o.ipaddress) == 0) {
        // return (timestamp.compareTo(o.timestamp));
        // } else
        return (codeTreatment.toString().compareTo(o.codeTreatment.toString()));
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MedicareWritable) {
            MedicareWritable other = (MedicareWritable) o;
            return codeTreatment.toString().equals(other.codeTreatment.toString());
            // && timestamp.equals(other.timestamp);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return codeTreatment.hashCode();
    }
    // public String toString() {
    // return codeTreatment.toString();
    // }

}