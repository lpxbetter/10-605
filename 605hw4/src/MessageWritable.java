import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MessageWritable implements Writable{
	private long Bx;
	private long Cx;
	private String message;
	
	public MessageWritable()
	{
		this.Bx = 0L;
		this.Cx = 0L;
		this.message = "";
	}
	
	public void readFields(DataInput data) {
		try {
			this.message=Text.readString(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			this.Bx=data.readLong();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			this.Cx=data.readLong();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void write(DataOutput output) {
		try {
			output.writeLong(Bx);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			output.writeLong(Cx);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			Text.writeString(output, this.message);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String toString(){
		return "M="+message+",Bx="+Bx+",Cx="+Cx;
	}
	//bg count get and set
	public void setBx(long val) {
		this.Bx = val;
	}
	public long getBx() {
		return this.Bx;
	}

	//fg count get and set
	public void setCx(long val) {
		this.Cx = val;
	}	
	public long getCx() {
		return this.Cx;
	}

	// Message get and set
	public void setMessage(String message){
		this.message = message;
	}
	public String getMessage(){
		return this.message;
	}

}