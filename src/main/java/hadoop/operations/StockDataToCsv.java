package hadoop.operations;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

import org.joda.time.*;
import hadoop.models.StockData;

public class StockDataToCsv extends DoFn<StockData, String> {

	@Override
	public void process(StockData data, Emitter<String> emitter) {
		// emit CSV formatted string
		String line = String.format("%s, %s, %s, %s", data.getDateAsJodaTime().toString("yyyy-MM"), 
			data.open, data.close, data.adjClose);

		emitter.emit(line);
	}
}