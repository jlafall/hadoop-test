package hadoop.operations;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

import org.joda.time.*;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import hadoop.models.StockData;

public class CsvToStockData extends DoFn<String, StockData> {
	private static final Splitter spliter = Splitter.onPattern(",").omitEmptyStrings();

	@Override
	public void process(String line, Emitter<StockData> emitter) {
		StockData data = new StockData();

		// get columns from row
		String[] columns = Iterables.toArray(spliter.split(line), String.class);
		
		// get column data and put into StockData object
		data.setDateFromJodaTime(new DateTime(columns[0].trim()));
		data.open = Double.parseDouble(columns[1].trim());
		data.close = Double.parseDouble(columns[4].trim());
		data.adjClose = Double.parseDouble(columns[6].trim());

		// emit stock data object, one per row
		emitter.emit(data);
	}
}