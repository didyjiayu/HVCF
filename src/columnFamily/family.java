package columnFamily;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class family {

    public String[] columnfamily(String address) throws IOException {
        FileInputStream input = new FileInputStream(address);
        DataInputStream data = new DataInputStream(input);
        InputStreamReader line = new InputStreamReader(data);
        BufferedReader br = new BufferedReader(line);

        String fLine;
        String[] cFamily = null;
        lableA:
        {
            while ((fLine = br.readLine()) != null) {
                //Just use the header row.
                if (fLine.contains("#CHROM")) {
                    cFamily = fLine.split("\t", -1);
                    for (int i = 0; i < cFamily.length; i++) {
                        cFamily[i] = cFamily[i].trim();
                    }
                    cFamily[0] = cFamily[0].substring(1);
                    //When have the columns names, stop the loop
                    break lableA;
                }
            }
        }
        br.close();
        return cFamily;
    }

}
