package columnType;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class columnType {

    public HashMap<String, String> typeOf(String address) throws IOException {

        FileInputStream input = new FileInputStream(address);
        DataInputStream data = new DataInputStream(input);
        InputStreamReader line = new InputStreamReader(data);
        BufferedReader br = new BufferedReader(line);

        String fLine;
        HashMap<String, String> map = new HashMap<String, String>();

        lableA:
        {
            while ((fLine = br.readLine()) != null) {
                if (fLine.contains("##INFO") && (fLine.contains("Number=1") | fLine.contains("Number=0"))) {
                    String infoLine = fLine.substring(8);
                    String[] info = infoLine.split(",", -1);
                    String[] idRow = info[0].trim().split("=");
                    String ID = idRow[1].trim();
                    String[] typeRow = info[2].trim().split("=");
                    String Type = typeRow[1].trim();
                    map.put(ID, Type);
                } else if (fLine.contains("##FORMAT") && (fLine.contains("Number=1") | fLine.contains("Number=0"))) {
                    String formatLine = fLine.substring(10);
                    String[] format = formatLine.split(",", -1);
                    String[] idRow = format[0].trim().split("=");
                    String ID = "format" + idRow[1].trim();
                    String[] typeRow = format[2].trim().split("=");
                    String Type = typeRow[1].trim();
                    map.put(ID, Type);
                } else if (!fLine.contains("##")) {
                    break lableA;
                }
            }
        }
        br.close();
        Iterator<Map.Entry<String, String>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if ("String".equalsIgnoreCase(entry.getValue())) {
                iter.remove();
            } else if ("Integer".equalsIgnoreCase(entry.getValue())) {
                entry.setValue("Int");
            } else if ("Flag".equalsIgnoreCase(entry.getValue())) {
                entry.setValue("Boolean");
            }
        }
        return map;

    }

}
