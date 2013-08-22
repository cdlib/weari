package org.cdlib.was.weari.pig;

import java.io.File;
import java.io.IOException;

import java.net.URL;
import java.net.JarURLConnection;

import java.util.HashMap;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.util.WrappedIOException;

import com.cybozu.labs.langdetect.*;

public class LANGDETECT extends EvalFunc<String> {
    static boolean isLoaded = false;
    static synchronized void loadProfile () {
        if (!isLoaded) {
            try {
                ResourceDetectorFactory.loadProfile();
                isLoaded = true;
            } catch (LangDetectException ex) {
                System.err.println(ex);
            }
        }
    }

    public String exec(Tuple input) throws IOException {
        LANGDETECT.loadProfile();
        if (input == null || input.size() == 0)
            return null;
        try {
            Detector detector = DetectorFactory.create();
            Object obj = input.get(0);
            int type = input.getType(0);
            String s = null;
            if (type == DataType.CHARARRAY) {
                s = (String)obj;
            } else if (type == DataType.BYTEARRAY) {
                s = ((DataByteArray)obj).toString();
            } else {
                s = "";
            }
            detector.append(s);
            return detector.detect();
        } catch(Exception e) {
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}
