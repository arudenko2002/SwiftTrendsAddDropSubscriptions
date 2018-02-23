package TrackAction;

import com.sun.media.jfxmedia.logging.Logger;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.json.JSONObject;

import java.util.ArrayList;

public class ParDoCombineLines implements SerializableFunction<Iterable<String>, String> {
    String dividor="&&&";
    @Override
    public String apply(Iterable<String> input) {
        int size=0;
        StringBuffer sb = new StringBuffer();
        for(String item: input) {
            sb.append(item+dividor);
            //System.out.println(item);
        }
        String out = sb.toString();
        return out;
    }
}
