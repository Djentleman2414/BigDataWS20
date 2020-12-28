package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class TextIntWritable implements WritableComparable<TextIntWritable> {

	private String text;
	private int i;
	
	public TextIntWritable() {
	}

	public TextIntWritable(String text, int i) {
		this.text = text;
		this.i = i;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, text);
		out.writeInt(i);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		text = WritableUtils.readString(in);
		i = in.readInt();
	}

	@Override
	public int compareTo(TextIntWritable o) {
		if(text == null)
			return -1;
		int dist = text.compareTo(o.text);
		if(dist != 0)
			return dist;
		
		return Integer.compare(i, o.i);
	}
	
	public void set(String text, int i) {
		this.text = text;
		this.i = i;
	}
	
	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getInt() {
		return i;
	}

	public void setInt(int i) {
		this.i = i;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + i;
		result = prime * result + ((text == null) ? 0 : text.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TextIntWritable other = (TextIntWritable) obj;
		if (i != other.i)
			return false;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(text).append('\t').append(i);
		
		return sb.toString();
	}
	
	public TextIntWritable clone() {
		return new TextIntWritable(text, i);
	}

}
