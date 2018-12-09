import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Item implements Writable{
	private Text fileName;
	private IntWritable count;
	
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		fileName.readFields(arg0);
		count.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		fileName.write(arg0);
		count.write(arg0);
	}
	
	public Item(Text fileName, IntWritable count) {
		this.fileName = fileName;
		this.count = count;
	}
	
	public Text getFileName() {
		return fileName;
	}

	public void setFileName(Text fileName) {
		this.fileName = fileName;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}
	
}
