/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Query;

import com.jidesoft.grid.SortableTable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class Exporter {

    public Exporter() {
    }

    public void exportTable(SortableTable table, File file) throws IOException {
        FileWriter out = new FileWriter(file);
        for (int i = 0; i < table.getColumnCount(); i++) {
            if (i ==0) out.write(table.getColumnName(i));
            else out.write("\t" + table.getColumnName(i));
        }
        out.write("\r\n");
        for (int i = 0; i < table.getRowCount(); i++) {
            for (int j = 0; j < table.getColumnCount(); j++) {
                if (table.getValueAt(i, j) != null) {
                    if (j==0) out.write(table.getValueAt(i, j).toString());
                    else out.write("\t"+ table.getValueAt(i, j).toString());
                } else {
                    if (j==0) out.write("null");
                    else out.write("\t"+"null");
                }
            }
            if (i<(table.getRowCount()-1))out.write("\r\n");
        }
        out.close();
    }
}
