/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Create;

import hiveConnection.hiveConnection;
import com.jidesoft.grid.AutoFilterTableHeader;
import com.jidesoft.list.DefaultDualListModel;
import createHiveTable.createHiveTable2;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.DefaultCellEditor;
import javax.swing.JComboBox;
import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 *
 * @author yujia1986
 */
public class pnl_create_hive_link extends javax.swing.JPanel {

    /**
     * Creates new form pnl_create_hive_link
     *
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     * @throws java.io.IOException
     */
    public pnl_create_hive_link() throws ClassNotFoundException, SQLException, IOException {
        initComponents();
        showHBaseTables runabletask1 = new showHBaseTables();
        new Thread(runabletask1).start();
        setDualList();
        showHiveTables();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jPanel2 = new javax.swing.JPanel();
        jScrollPane2 = new javax.swing.JScrollPane();
        tbl_hbase_tables = new com.jidesoft.grid.SortableTable();
        jPanel1 = new javax.swing.JPanel();
        jScrollPane1 = new javax.swing.JScrollPane();
        list_dual_hive_column_family = new com.jidesoft.list.DualList();
        createTable = new javax.swing.JButton();
        jPanel3 = new javax.swing.JPanel();
        jScrollPane3 = new javax.swing.JScrollPane();
        list_hive_tables = new javax.swing.JList();

        jScrollPane2.setBorder(javax.swing.BorderFactory.createTitledBorder("HBase Tables Available"));

        tbl_hbase_tables.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {

            }
        ));
        jScrollPane2.setViewportView(tbl_hbase_tables);

        javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
        jPanel2.setLayout(jPanel2Layout);
        jPanel2Layout.setHorizontalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jScrollPane2, javax.swing.GroupLayout.DEFAULT_SIZE, 624, Short.MAX_VALUE)
                .addContainerGap())
        );
        jPanel2Layout.setVerticalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jScrollPane2, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 302, Short.MAX_VALUE)
        );

        list_dual_hive_column_family.setBorder(javax.swing.BorderFactory.createTitledBorder("Choose Column Families"));
        list_dual_hive_column_family.setVisibleRowCount(5);
        jScrollPane1.setViewportView(list_dual_hive_column_family);

        createTable.setText("Create");
        createTable.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                createTableActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addGap(0, 551, Short.MAX_VALUE)
                        .addComponent(createTable))
                    .addComponent(jScrollPane1))
                .addContainerGap())
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 222, Short.MAX_VALUE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(createTable))
        );

        jScrollPane3.setBorder(javax.swing.BorderFactory.createTitledBorder("Hive Tables Available"));

        jScrollPane3.setViewportView(list_hive_tables);

        javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
        jPanel3.setLayout(jPanel3Layout);
        jPanel3Layout.setHorizontalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel3Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jScrollPane3)
                .addContainerGap())
        );
        jPanel3Layout.setVerticalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel3Layout.createSequentialGroup()
                .addComponent(jScrollPane3, javax.swing.GroupLayout.DEFAULT_SIZE, 100, Short.MAX_VALUE)
                .addContainerGap())
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jPanel2, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addComponent(jPanel1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addComponent(jPanel3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(jPanel2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jPanel3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
    }// </editor-fold>//GEN-END:initComponents

    private void createTableActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_createTableActionPerformed
        // TODO add your handling code here:
        try {
            Connection hiveConnection = this.getHiveConnection();
            String selectedTableName = selectedTableName();
            String[] selectedTableFamilies = selectedTableFamilies();
            String[] selectedFamilies = selectedFamilies();
            String newTableName = JOptionPane.showInputDialog("Please give a table name:", selectedTableName);
            JTable tableType = new JTable();
            DefaultTableModel tableTypeModel = (DefaultTableModel) tableType.getModel();
            setMapTable(tableType, tableTypeModel, selectedFamilies);
            int type = JOptionPane.showConfirmDialog(null, tableType, "Please give the column type", JOptionPane.OK_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null);
            if (type == 2) {
                return;
            }
            HashMap<String, String> map = getTypePair(tableTypeModel);
            createHiveTable2 createExternalTable = new createHiveTable2();
            createExternalTable.externalTable(hiveConnection, selectedTableName, newTableName, selectedFamilies, map);
            DefaultDualListModel dualModel = new DefaultDualListModel();
            dualModel = (DefaultDualListModel) list_dual_hive_column_family.getModel();
            dualModel.removeAllElements();
            list_dual_hive_column_family.setModel(dualModel);
            showHiveTables();
        } catch (IOException | ClassNotFoundException | SQLException ex) {
            Logger.getLogger(pnl_create_hive_link.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_createTableActionPerformed

    public class showHBaseTables implements Runnable {

        public HTableDescriptor[] listHBaseTables() throws IOException {
            Configuration conf = HBaseConfiguration.create();
            HBaseAdmin admin = new HBaseAdmin(conf);
            return admin.listTables();
        }

        private void showHBaseTables() {
            try {
                // TODO add your handling code here:
                HTableDescriptor[] tablesDescriptor = this.listHBaseTables();
                DefaultTableModel model = new DefaultTableModel();
                model.addColumn("Table Name");
                model.addColumn("Table Columnfamily Names");
                AutoFilterTableHeader _header = new AutoFilterTableHeader(tbl_hbase_tables);
                _header.setAutoFilterEnabled(true);
                _header.setReorderingAllowed(false);
                _header.setUseNativeHeaderRenderer(true);
                tbl_hbase_tables.setTableHeader(_header);
                for (HTableDescriptor descriptor : tablesDescriptor) {
                    ArrayList<String> row = new ArrayList<>();
                    row.add(descriptor.getNameAsString());
                    StringBuilder builder = new StringBuilder();
                    HColumnDescriptor[] cDescriptor = descriptor.getColumnFamilies();
                    for (int i = 0; i < cDescriptor.length; i++) {
                        if (i > 0) {
                            builder.append(", ");
                        }
                        builder.append(cDescriptor[i].getNameAsString());
                    }
                    row.add(builder.toString());
                    String[] rowArray = row.toArray(new String[row.size()]);
                    model.addRow(rowArray);
                }
                tbl_hbase_tables.setModel(model);
            } catch (IOException ex) {
                Logger.getLogger(pnl_create_hive_link.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public void run() {
            showHBaseTables();
        }
    }

    public Connection getHiveConnection() throws ClassNotFoundException, SQLException, IOException {
        FileInputStream input = new FileInputStream("/tmp/HVCF/hiveServerInfo/hiveServerInfo.txt");
        DataInputStream data = new DataInputStream(input);
        InputStreamReader line = new InputStreamReader(data);
        BufferedReader br = new BufferedReader(line);
        String fLine;
        String[] info = null;
        while ((fLine = br.readLine()) != null) {
            info = fLine.split(",", -1);
        }
        hiveConnection connection = new hiveConnection();
        return connection.connectHive(info[0], info[1], info[2], info[3]);
    }

    public void showHiveTables() throws ClassNotFoundException, SQLException, IOException {
        Connection con = this.getHiveConnection();
        Statement stmt = con.createStatement();
        ArrayList<String> hiveList = new ArrayList<>();
        ResultSet res = stmt.executeQuery("show tables");
        while (res.next()) {
            hiveList.add(res.getString(1));
        }
        String hiveTableArray[] = hiveList.toArray(new String[hiveList.size()]);
        list_hive_tables.setListData(hiveTableArray);
    }

    public String selectedTableName() {
        String selectTableName = tbl_hbase_tables.getValueAt(tbl_hbase_tables.getSelectedRow(), 0).toString();
        return selectTableName;
    }

    public String[] selectedTableFamilies() {
        String selectTableFamilies = tbl_hbase_tables.getValueAt(tbl_hbase_tables.getSelectedRow(), 1).toString();
        String[] families = selectTableFamilies.split(",", -1);
        for (int i = 0; i < families.length; i++) {
            families[i] = families[i].trim();
        }
        return families;
    }

    public String[] selectedFamilies() {
        Object[] selected = list_dual_hive_column_family.getSelectedValues();
        ArrayList<String> selectedFamiliesList = new ArrayList<>();
        for (Object c : selected) {
            selectedFamiliesList.add(c.toString());
        }
        String[] selectedFamilies = selectedFamiliesList.toArray(new String[selectedFamiliesList.size()]);
//        for (int i = 0; i < selectedFamilies.length; i++) {
//            String[] tmp = selectedFamilies[i].split(",", -1);
//            selectedFamilies[i] = tmp[0];
//        }
        return selectedFamilies;
    }

    public void setDualList() throws FileNotFoundException {
        tbl_hbase_tables.setRowSelectionAllowed(true);
        ListSelectionModel rowSelectionModel = tbl_hbase_tables.getSelectionModel();
        rowSelectionModel.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        rowSelectionModel.addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent e) {
                if (!e.getValueIsAdjusting()) {
                    try {
                        FileInputStream input = null;
                        String[] selectedTableFamilies = selectedTableFamilies();
                        String selectedTableName = selectedTableName();
                        DefaultDualListModel dualModel = new DefaultDualListModel();
                        String path = System.getProperty("user.dir");
                        path = path + "/HVCF/HBase/" + selectedTableName + "/columns.txt";
                        input = new FileInputStream(path);
                        DataInputStream data = new DataInputStream(input);
                        InputStreamReader line = new InputStreamReader(data);
                        BufferedReader br = new BufferedReader(line);
                        String fLine;
                        String[] columns = null;
                        while ((fLine = br.readLine()) != null) {
                            columns = fLine.split(",", -1);
                        }
                        ArrayList<String> list = new ArrayList<>();
                        for (int i = 1; i < columns.length - 1; i++) {
                            list.add(columns[i]);
                        }
                        Collections.sort(list);
                        for (String c : list) {
                            dualModel.addElement(c);
                        }
                        list_dual_hive_column_family.setModel(dualModel);
                    } catch (FileNotFoundException ex) {
                        Logger.getLogger(pnl_create_hive_link.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (IOException ex) {
                        Logger.getLogger(pnl_create_hive_link.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        });
    }

    public void setMapTable(JTable table, DefaultTableModel model, String[] selectedFamilies) {
        String[] types = {"String", "Int", "Float", "Boolean"};
        model.setRowCount(0);
        model.addColumn("Column");
        model.addColumn("Type");
        TableColumn sportColumn = table.getColumnModel().getColumn(1);
        JComboBox comboBox = new JComboBox();
        for (String a : types) {
            comboBox.addItem(a);
        }
        sportColumn.setCellEditor(new DefaultCellEditor(comboBox));
        comboBox.setSelectedIndex(0);
        for (String b : selectedFamilies) {
            String[] tmp = new String[1];
            tmp[0] = b;
            model.addRow(tmp);
        }
    }
    
    public HashMap<String, String> getTypePair(DefaultTableModel model) {
        HashMap<String, String> map = new HashMap<>();
        for (int i = 0; i < model.getRowCount(); i++) {
            String column = model.getValueAt(i, 0).toString();
            String type = model.getValueAt(i, 1).toString();
            map.put(column, type);
        }
        return map;
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton createTable;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JScrollPane jScrollPane3;
    private com.jidesoft.list.DualList list_dual_hive_column_family;
    private javax.swing.JList list_hive_tables;
    private com.jidesoft.grid.SortableTable tbl_hbase_tables;
    // End of variables declaration//GEN-END:variables
}
