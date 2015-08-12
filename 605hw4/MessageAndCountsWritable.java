import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class MessageAndCountsWritable implements Writable{
	private String message;
	private long Bx;
	private long Cx;
	
	public MessageAndCountsWritable()
	{
		this.message = "";
		this.Bx = 0L;
		this.Cx = 0L;
	}
	
	public String getMessage(){
		return this.message;
	}
	
	public void setMessage(String m){
		this.message = m;
	}
	
	public long getBx() {
		return this.Bx;
	}

	public void setBx(long bx) {
		this.Bx = bx;
	}
	
	public long getCx() {
		return this.Cx;
	}

	public void setCx(long cx) {
		this.Cx = cx;
	}


	public void readFields(DataInput in) throws IOException {
		this.message=Text.readString(in);
		this.Bx=in.readLong();
		this.Cx=in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.message);
		out.writeLong(Bx);
		out.writeLong(Cx);
	}
	
	public String toString(){
		return "Message="+message+",Bx="+Bx+",Cx="+Cx;
	}
}