/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package subColumns;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 *
 * @author yujia1986
 */
public class subColumns {

    public String[] columns(String filePath) throws FileNotFoundException, IOException {
        FileInputStream input = new FileInputStream(filePath);
        DataInputStream data = new DataInputStream(input);
        InputStreamReader line = new InputStreamReader(data);
        BufferedReader br = new BufferedReader(line);

        String fLine;
        ArrayList<String> subFamilyColumns = new ArrayList<>();

        lableA:
        {
            while ((fLine = br.readLine()) != null) {
                if (fLine.contains("##")) {
                    if (fLine.contains("##INFO")) {
                        String infoLine = fLine.substring(8);
                        String[] info = infoLine.split(",", -1);
                        String[] idRow = info[0].trim().split("=");
                        String ID = idRow[1].trim();
                        subFamilyColumns.add(ID);
                    } else if (fLine.contains("##FORMAT")) {
                        String formatLine = fLine.substring(10);
                        String[] format = formatLine.split(",", -1);
                        String[] idRow = format[0].trim().split("=");
                        String ID = "format" + idRow[1].trim();
                        subFamilyColumns.add(ID);
                    }
                } else {
                    break lableA;
                }
            }
        }
        String[] columns = subFamilyColumns.toArray(new String [subFamilyColumns.size()]);
        return columns;
    }

}
