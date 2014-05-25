/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Query;

import hiveConnection.hiveConnection;
import com.jidesoft.database.ResultSetTableModel;
import com.jidesoft.grid.AutoFilterTableHeader;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFileChooser;

/**
 *
 * @author yujia1986
 */
public class pnl_hive_query extends javax.swing.JPanel {

    /**
     * Creates new form hiveQuery
     */
    public pnl_hive_query() {
        try {
            initComponents();
            showHiveTables();
        } catch (ClassNotFoundException | SQLException | IOException ex) {
            Logger.getLogger(pnl_hive_query.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jPanel1 = new javax.swing.JPanel();
        jScrollPane1 = new javax.swing.JScrollPane();
        list_hive_tables = new javax.swing.JList();
        viewHiveTable = new javax.swing.JButton();
        deleteHiveTable = new javax.swing.JButton();
        jPanel2 = new javax.swing.JPanel();
        jScrollPane2 = new javax.swing.JScrollPane();
        hiveTable = new com.jidesoft.grid.SortableTable();
        jPanel6 = new javax.swing.JPanel();
        jScrollPane5 = new javax.swing.JScrollPane();
        query = new javax.swing.JTextArea();
        submitQuery = new javax.swing.JButton();
        btn_export = new javax.swing.JButton();
        jPanel5 = new javax.swing.JPanel();
        jScrollPane6 = new javax.swing.JScrollPane();
        queryResult = new com.jidesoft.grid.SortableTable();

        list_hive_tables.setBorder(javax.swing.BorderFactory.createTitledBorder("Hive Tables Available"));
        jScrollPane1.setViewportView(list_hive_tables);

        viewHiveTable.setText("View");
        viewHiveTable.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                viewHiveTableActionPerformed(evt);
            }
        });

        deleteHiveTable.setText("Delete");
        deleteHiveTable.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                deleteHiveTableActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jScrollPane1)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(deleteHiveTable, javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(viewHiveTable, javax.swing.GroupLayout.Alignment.TRAILING))
                .addContainerGap())
        );

        jPanel1Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {deleteHiveTable, viewHiveTable});

        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addComponent(viewHiveTable)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 54, Short.MAX_VALUE)
                        .addComponent(deleteHiveTable))
                    .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 0, Short.MAX_VALUE))
                .addContainerGap())
        );

        hiveTable.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {

            }
        ));
        jScrollPane2.setViewportView(hiveTable);

        javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
        jPanel2.setLayout(jPanel2Layout);
        jPanel2Layout.setHorizontalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jScrollPane2, javax.swing.GroupLayout.DEFAULT_SIZE, 608, Short.MAX_VALUE)
                .addContainerGap())
        );
        jPanel2Layout.setVerticalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jScrollPane2, javax.swing.GroupLayout.DEFAULT_SIZE, 26, Short.MAX_VALUE)
        );

        query.setColumns(20);
        query.setRows(3);
        query.setBorder(javax.swing.BorderFactory.createTitledBorder("Edit Query"));
        jScrollPane5.setViewportView(query);

        submitQuery.setText("Submit");
        submitQuery.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                submitQueryActionPerformed(evt);
            }
        });

        btn_export.setText("Export Result To");
        btn_export.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btn_exportActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel6Layout = new javax.swing.GroupLayout(jPanel6);
        jPanel6.setLayout(jPanel6Layout);
        jPanel6Layout.setHorizontalGroup(
            jPanel6Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel6Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jScrollPane5)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel6Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(btn_export, javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(submitQuery, javax.swing.GroupLayout.Alignment.TRAILING))
                .addContainerGap())
        );

        jPanel6Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {btn_export, submitQuery});

        jPanel6Layout.setVerticalGroup(
            jPanel6Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel6Layout.createSequentialGroup()
                .addComponent(submitQuery)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(btn_export))
            .addComponent(jScrollPane5, javax.swing.GroupLayout.DEFAULT_SIZE, 121, Short.MAX_VALUE)
        );

        queryResult.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {

            }
        ));
        jScrollPane6.setViewportView(queryResult);

        javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
        jPanel5.setLayout(jPanel5Layout);
        jPanel5Layout.setHorizontalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel5Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jScrollPane6)
                .addContainerGap())
        );
        jPanel5Layout.setVerticalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jScrollPane6, javax.swing.GroupLayout.DEFAULT_SIZE, 151, Short.MAX_VALUE)
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jPanel1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addComponent(jPanel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addComponent(jPanel6, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addComponent(jPanel5, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jPanel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jPanel6, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jPanel5, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap())
        );
    }// </editor-fold>//GEN-END:initComponents

    private void deleteHiveTableActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_deleteHiveTableActionPerformed
        // TODO add your handling code here:
        Object object = list_hive_tables.getSelectedValue();
        String tableName = object.toString();
        try {
            Connection hiveConnection = null;
            try {
                hiveConnection = this.getHiveConnection();
            } catch (IOException ex) {
                Logger.getLogger(pnl_hive_query.class.getName()).log(Level.SEVERE, null, ex);
            }
            Statement stmt = hiveConnection.createStatement();
            stmt.execute("drop table " + tableName);
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(pnl_hive_query.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_deleteHiveTableActionPerformed

    private void viewHiveTableActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_viewHiveTableActionPerformed
        // TODO add your handling code here:
        Object object = list_hive_tables.getSelectedValue();
        String tableName = object.toString();
        try {
            Connection hiveConnection = null;
            try {
                hiveConnection = this.getHiveConnection();
            } catch (IOException ex) {
                Logger.getLogger(pnl_hive_query.class.getName()).log(Level.SEVERE, null, ex);
            }
            Statement stmt = hiveConnection.createStatement();
            ResultSet res = stmt.executeQuery("select * from " + tableName);
            AutoFilterTableHeader _header = new AutoFilterTableHeader(hiveTable);
            _header.setAutoFilterEnabled(true);
            _header.setReorderingAllowed(false);
            _header.setUseNativeHeaderRenderer(true);
            hiveTable.setTableHeader(_header);
            ResultSetTableModel tableModel = new ResultSetTableModel(res);
            hiveTable.setModel(tableModel);
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(pnl_hive_query.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_viewHiveTableActionPerformed

    private void submitQueryActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_submitQueryActionPerformed
        // TODO add your handling code here:
        String queryInput = query.getText();
        try {
            Connection hiveConnection = null;
            try {
                hiveConnection = this.getHiveConnection();
            } catch (IOException ex) {
                Logger.getLogger(pnl_hive_query.class.getName()).log(Level.SEVERE, null, ex);
            }
            Statement stmt = hiveConnection.createStatement();
            ResultSet res = stmt.executeQuery(queryInput);
            AutoFilterTableHeader _header = new AutoFilterTableHeader(queryResult);
            _header.setAutoFilterEnabled(true);
            _header.setReorderingAllowed(false);
            _header.setUseNativeHeaderRenderer(true);
            queryResult.setTableHeader(_header);
            ResultSetTableModel tableModel = new ResultSetTableModel(res);
            queryResult.setModel(tableModel);
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(pnl_hive_query.class.getName()).log(Level.SEVERE, null, ex);
        }

    }//GEN-LAST:event_submitQueryActionPerformed

    private void btn_exportActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btn_exportActionPerformed
        // TODO add your handling code here:
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("Specify a file to save");
        int userSelection = chooser.showSaveDialog(null);

        if (userSelection == JFileChooser.APPROVE_OPTION) {
            try {
                File fileToSave = chooser.getSelectedFile();
                Exporter exp = new Exporter();
                exp.exportTable(queryResult, fileToSave);
            } catch (IOException ex) {
                Logger.getLogger(pnl_hive_query.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }//GEN-LAST:event_btn_exportActionPerformed

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

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btn_export;
    private javax.swing.JButton deleteHiveTable;
    private com.jidesoft.grid.SortableTable hiveTable;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JPanel jPanel5;
    private javax.swing.JPanel jPanel6;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JScrollPane jScrollPane5;
    private javax.swing.JScrollPane jScrollPane6;
    private javax.swing.JList list_hive_tables;
    private javax.swing.JTextArea query;
    private com.jidesoft.grid.SortableTable queryResult;
    private javax.swing.JButton submitQuery;
    private javax.swing.JButton viewHiveTable;
    // End of variables declaration//GEN-END:variables
}
