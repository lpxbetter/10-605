import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountDataWritable implements Writable{
	private long Bx;
	private long By;
	private long Bxy;
	private long Cx;
	private long Cy;
	private long Cxy;
	
	public CountDataWritable()
	{
		this.Bx = 0L;
		this.By = 0L;
		this.Bxy = 0L;
		this.Cx = 0L;
		this.Cy = 0L;
		this.Cxy = 0L;
	}

	public long getBx() {
		return this.Bx;
	}

	public void setBx(long bx) {
		this.Bx = bx;
	}

	public long getBy() {
		return this.By;
	}

	public void setBy(long by) {
		this.By = by;
	}

	public long getBxy() {
		return this.Bxy;
	}

	public void setBxy(long bxy) {
		this.Bxy = bxy;
	}

	public long getCx() {
		return this.Cx;
	}

	public void setCx(long cx) {
		this.Cx = cx;
	}

	public long getCy() {
		return this.Cy;
	}

	public void setCy(long cy) {
		this.Cy = cy;
	}
	
	public long getCxy() {
		return this.Cxy;
	}

	public void setCxy(long cxy) {
		this.Cxy = cxy;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.Bx=in.readLong();
		this.By=in.readLong();
		this.Bxy=in.readLong();
		this.Cx=in.readLong();
		this.Cy=in.readLong();
		this.Cxy=in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(Bx);
		out.writeLong(By);
		out.writeLong(Bxy);
		out.writeLong(Cx);
		out.writeLong(Cy);
		out.writeLong(Cxy);
	}
	
	public void addTo(CountDataWritable other){
		this.Bx += other.Bx;
		this.By += other.By;
		this.Bxy += other.Bxy;
		this.Cx += other.Cx;
		this.Cy += other.Cy;
		this.Cxy += other.Cxy;
	}
	
	public String toString(){
		return "Bx="+Bx+",By="+By+",Bxy="+Bxy+",Cx="+Cx+",Cy="+Cy+",Cxy="+Cxy;
	}
	
}