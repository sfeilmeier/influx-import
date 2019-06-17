import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.dhatim.fastexcel.reader.ReadableWorkbook;
import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.Sheet;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

public class InfluxImportApp {

	private final static String INFLUX_DB = "db";
	private final static String INFLUX_RETENTION_POLICY = "autogen";
	private final static String INFLUX_URL = "http://localhost:8086";
	private final static String INFLUX_USER = "root";
	private final static String INFLUX_PASSWORD = "root";
	private final static String INFLUX_MEASUREMENT = "data";

	private final static int EDGE_ID = 0;
	private final static String CHANNEL_ADDRESS = "_sum/ProductionActivePower";

	public static void main(String[] args) throws FileNotFoundException, IOException {
		Map<LocalDateTime, Double> data = readExcel();
		writeToInfluxdb(data);
	}

	private static void writeToInfluxdb(Map<LocalDateTime, Double> data) {
		InfluxDB influxDB = InfluxDBFactory.connect(INFLUX_URL, INFLUX_USER, INFLUX_PASSWORD);
		influxDB.query(new Query("CREATE DATABASE " + INFLUX_DB));
		influxDB.query(new Query("CREATE RETENTION POLICY " + INFLUX_RETENTION_POLICY + " ON " + INFLUX_DB
				+ " DURATION 30h REPLICATION 2 DEFAULT"));

		BatchPoints batchPoints = BatchPoints.database(INFLUX_DB).tag("async", "true")
				.retentionPolicy(INFLUX_RETENTION_POLICY).consistency(ConsistencyLevel.ALL).build();

		int counter = 0;
		for (Entry<LocalDateTime, Double> entry : data.entrySet()) {
			Point point = Point.measurement(INFLUX_MEASUREMENT).tag("edge", String.valueOf(EDGE_ID)) //
					.time(entry.getKey().toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS)
					.addField(CHANNEL_ADDRESS, entry.getValue()).build();
			batchPoints.point(point);

			if (counter++ % 10000 == 0) {
				System.out.println("Write batchpoints. " + counter);
				influxDB.write(batchPoints);
				batchPoints = BatchPoints.database(INFLUX_DB).tag("async", "true")
						.retentionPolicy(INFLUX_RETENTION_POLICY).consistency(ConsistencyLevel.ALL).build();
			}
		}
		influxDB.write(batchPoints);
	}

	private static Map<LocalDateTime, Double> readExcel() throws FileNotFoundException, IOException {
		Map<LocalDateTime, Double> data = new HashMap<>();
		try (InputStream is = new FileInputStream(new File("doc/PV-historisch Süd 30Grad.xlsx"));
				ReadableWorkbook wb = new ReadableWorkbook(is)) {
			Sheet sheet = wb.getFirstSheet();

			boolean isFirst = true;
			for (Row row : sheet.read()) {
				if (isFirst) {
					isFirst = false;
					continue;
				}
				LocalDateTime date = row.getCellAsDate(0).orElse(null);
				double value = row.getCellAsNumber(1).orElse(null).doubleValue();
				data.put(date, value);
			}
		}
		return data;
	}

}
