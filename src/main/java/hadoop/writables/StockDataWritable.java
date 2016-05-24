// package hadoop.writables;

// import org.apache.hadoop.io.*;
// import org.joda.time.*;
// import hadoop.models.StockData;

// public class StockDataWritable extends WritableComparable <StockDataWritable> {
// 	private Text date;
// 	private DoubleWritable open;
// 	private DoubleWritable close;
// 	private DoubleWritable adjClose;

// 	public StockDataWritable() {
// 		set(new StockData());
// 	}	

// 	public StockDataWritable(StockData data) {
// 		set(data);
// 	}

// 	public void set(StockData data) {
// 		date.set(data);
// 	}

// 	public StockData get() {
// 		StockData data = new StockData();
// 		return data;
// 	}
// }