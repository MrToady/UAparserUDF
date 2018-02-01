import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

public class UAParser extends GenericUDTF {
    private transient Object forwardColObj[] = new Object[3];
    private transient ObjectInspector[] inputOIs;
    private UserAgent userAgent;

    /**
     * @param argOIs check the argument is valid.
     * @return the output column structure.
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        inputOIs = argOIs;
        List<String> outFieldNames = new ArrayList<>();
        List<ObjectInspector> outFieldOIs = new ArrayList<>();

        outFieldNames.add("device");
        outFieldNames.add("os");
        outFieldNames.add("browser");
        outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String userAgentString = ((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(objects[0]);
        userAgent = new UserAgent(userAgentString);
        forwardColObj[0] = userAgent.getBrowser().getBrowserType().getName();
        forwardColObj[1] = userAgent.getOperatingSystem().getName();
        forwardColObj[2] = userAgent.getBrowser().getName();
        forward(forwardColObj);
    }


    @Override
    public void close() throws HiveException {
    }
}