package myudfs;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class Sample_Eval extends EvalFunc<String> {

    @Override
    public String exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() ==0){
            return null;
        }
        String str = tuple.get(0).toString();
        return str.toUpperCase();
    }
}