import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.example.PB.Event;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.example.Main.COLUMN_NAME;
import static org.example.Main.getLastEventInFilter;
import static org.example.Main.writeMessages;

class MainTest {


    @Test
    void testFail() throws IOException {
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
        int x1val = ((Event.ScalarInt.Builder) x1).getValue();
        // Read file with another filter to not the end of the file
        var x2 = getLastEventInFilter(hadoopInputFile, subFilter);
        int x2val = ((Event.ScalarInt.Builder) x2).getValue();

        // show the last event read is now set to the event at the end of the file instead of end of filter

        Assertions.assertNotEquals(x1val, x2val);
    }
}
