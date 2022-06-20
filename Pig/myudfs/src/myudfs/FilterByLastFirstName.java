package myudfs;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class FilterByLastFirstName extends FilterFunc {
    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0)
            return null;
        String lastName = tuple.get(1).toString().toLowerCase();
        String first_name = tuple.get(2).toString().toLowerCase();
        return lastName.startsWith(String.valueOf(first_name.charAt(0)));
    }

}
