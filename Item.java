import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Item implements Writable{
	private String fileName;
	private int count;

	public Item() {
		
	}
	
	public Item(String fileName, int count) {
		this.fileName = fileName;
		this.count = count;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// Auto-generated method stub
		fileName = arg0.readUTF();
		count = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// Auto-generated method stub
		arg0.writeUTF(fileName);
		arg0.writeInt(count);
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}
