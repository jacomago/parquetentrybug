package org.example;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.example.PB.Event;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        stuff(); System.out.println("Hello world!");
    }

    public static String COLUMN_NAME = "value";


    /**
     * Get the last event from a file filtered by an input filter
     *
     * @param hadoopInputFile file
     * @param filter filter for the file
     * @return last event before end and after start
     * @throws IOException if reading file fails
     */
    public static Message.Builder getLastEventInFilter(
            InputFile hadoopInputFile, FilterCompat.Filter filter)
            throws IOException {
        try (var reader =
                     ProtoParquetReader.builder(hadoopInputFile).withFilter(filter).build()) {
            var value = reader.read();
            // Need clone because reader does some caching that breaks this somehow
            var prevValue = value != null ? ((Message.Builder) value) : null;

            while (value != null) {
                logger.info("prevValue " +  prevValue);
                logger.info("value " +  prevValue);
                prevValue = ((Message.Builder) value);
                value = reader.read();
            }
            logger.info("l prevValue " +  prevValue);
            logger.info("l value " +  prevValue);
            return prevValue;
        }

    }
    public static <T extends Message> void writeMessages(org.apache.hadoop.fs.Path file, List<T> messages) throws IOException {

        if (messages.isEmpty()) return;
        ProtoParquetWriter.Builder<Object> builder =
                ProtoParquetWriter.builder(file).withMessage(messages.getFirst().getClass()).withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
        try (var writer = builder.build()) {
            for (T message : messages) {
                writer.write(message);
            }
        }
    }


    public static void stuff() throws IOException {
        // Create file with single column entries 1, 2, 3, 4...
        var entries = Stream.of(1,2,3,4,5,6,7,8).map(i -> Event.ScalarInt.newBuilder().setValue(i).build()).collect(Collectors.toList());
        var path = new Path("test.parquet");
        var hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());

        writeMessages(hadoopPath, entries);
        // Create filters on the file
        FilterPredicate gtPredicate = gt(intColumn(COLUMN_NAME), 5);
        FilterCompat.Filter gtFilter = FilterCompat.get(gtPredicate);
        // Create another filter that is more restrictive
        FilterPredicate subPredicate = and(lt(intColumn(COLUMN_NAME), 5), gt(intColumn(COLUMN_NAME), 2));
        FilterCompat.Filter subFilter = FilterCompat.get(subPredicate);
        // Read file with one filter to the end of the file
        var config = new Configuration();
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(hadoopPath, config);
        var x1 = getLastEventInFilter(hadoopInputFile, gtFilter);
        // Read file with another filter to not the end of the file
        var x2 = getLastEventInFilter(hadoopInputFile, subFilter);
        // show the last event read is now set to the event at the end of the file instead of end of filter
        assert x1 == x2;

    }
}