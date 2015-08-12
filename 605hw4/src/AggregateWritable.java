import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AggregateWritable implements Writable{
	private long Bx;
	private long Cx;
	private long Bxy;
	private long Cxy;
	private long By;
	private long Cy;
	
	public AggregateWritable()
	{
		this.Bx = 0L;
		this.Cx = 0L;
		this.Bxy = 0L;
		this.Cxy = 0L;
		this.By = 0L;
		this.Cy = 0L;
	}
	public String toString(){
		return "Bx="+Bx+",By="+By+",Bxy="+Bxy+",Cx="+Cx+",Cy="+Cy+",Cxy="+Cxy;
	}

	public void readFields(DataInput data) throws IOException {
		this.Bx=data.readLong();
		this.Cx=data.readLong();
		this.Bxy=data.readLong();
		this.Cxy=data.readLong();
		this.By=data.readLong();
		this.Cy=data.readLong();
	}

	public void write(DataOutput data) throws IOException {
		data.writeLong(Bx);
		data.writeLong(Cx);
		data.writeLong(Bxy);
		data.writeLong(Cxy);
		data.writeLong(By);
		data.writeLong(Cy);
	}
	
	//bx set and get
	public void setBx(long bx) {
		this.Bx = bx;
	}
	public long getBx() {
		return this.Bx;
	}
	//cx get and set
	public void setCx(long val) {
		this.Cx = val;
	}
	public long getCx() {
		return this.Cx;
	}

	//bxy set and get
	public void setBxy(long val) {
		this.Bxy = val;
	}
	public long getBxy() {
		return this.Bxy;
	}
	//cxy set and get 
	public void setCxy(long val) {
		this.Cxy = val;
	}
	public long getCxy() {
		return this.Cxy;
	}
	
    //by get and set	
	public void setBy(long val) {
		this.By = val;
	}
	public long getBy() {
		return this.By;
	}
	// cy set and get
	public void setCy(long val) {
		this.Cy = val;
	}
	public long getCy() {
		return this.Cy;
	}
	
	public void plus(AggregateWritable w2){
		this.Bx += w2.Bx;
		this.Cx += w2.Cx;
		this.Bxy += w2.Bxy;
		this.Cxy += w2.Cxy;		
		this.By += w2.By;
		this.Cy += w2.Cy;
	}
	
}