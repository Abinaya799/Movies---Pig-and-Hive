package myudfs;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class LargestSummary extends EvalFunc<Long> implements Algebraic {
    @Override
    public String getInitial() {
        return null;
    }

    @Override
    public String getIntermed() {
        return null;
    }

    @Override
    public String getFinal() {
        return null;
    }

    @Override
    public Long exec(Tuple tuple) throws IOException {
        return null;
    }
}
