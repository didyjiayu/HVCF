/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Main;

import Create.pnl_create_hbase_table;
import Create.pnl_create_hive_link;
import Import.pnl_import_vcf;
import Manage.pnl_manage_hbase_tables;
import Manage.pnl_manage_hive_link;
import Query.pnl_hive_query;
import Server.pnl_hadoop_server;
import Server.pnl_hbase_server;
import Server.pnl_hive_server;
import com.jidesoft.action.DefaultDockableBarDockableHolder;
import com.jidesoft.plaf.LookAndFeelFactory;
import com.jidesoft.status.LabelStatusBarItem;
import com.jidesoft.status.MemoryStatusBarItem;
import com.jidesoft.swing.JideBoxLayout;
import com.jidesoft.swing.JideTabbedPane;
import java.awt.Dimension;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JLabel;

/**
 *
 * @author aardes0
 */
public class Frm_Main extends DefaultDockableBarDockableHolder {

    /**
     * Creates new form NewJFrame
     */
    public Frm_Main() {

        initComponents();

        LookAndFeelFactory.installJideExtension(LookAndFeelFactory.OFFICE2007_STYLE);
        JIDP_Main.setTabShape(JideTabbedPane.SHAPE_OFFICE2003);
        JIDP_Main.setColorTheme(JideTabbedPane.COLOR_THEME_WIN2K);
        setIcon();
        createStatusBar();
    }

    private void setIcon() {
        setTitle("HVCF");
        //   setIconImage(Toolkit.getDefaultToolkit().getImage(getClass().getResource("Icon.png")));

    }

    public void ShowFrmI(ActionEvent evt) {
        String Frm = evt.toString().substring(evt.toString().indexOf("text") + 5, evt.toString().length() - 1);
        switch (Frm) {
            case "Hadoop": {
                JIDP_Main.addTab(Frm, null, new pnl_hadoop_server());
                JIDP_Main.setSelectedIndex(JIDP_Main.getTabCount() - 1);
                break;
            }
            case "HBase": {
                JIDP_Main.addTab(Frm, null, new pnl_hbase_server());
                JIDP_Main.setSelectedIndex(JIDP_Main.getTabCount() - 1);
                break;
            }
            case "Hive": {
                JIDP_Main.addTab(Frm, null, new pnl_hive_server());
                JIDP_Main.setSelectedIndex(JIDP_Main.getTabCount() - 1);
                break;
            }
            case "HBase Tables": {
                JIDP_Main.addTab(Frm, null, new pnl_manage_hbase_tables());
                JIDP_Main.setSelectedIndex(JIDP_Main.getTabCount() - 1);
                break;
            }
            case "HiveQuery": {
                JIDP_Main.addTab(Frm, null, new pnl_hive_query());
                JIDP_Main.setSelectedIndex(JIDP_Main.getTabCount() - 1);
                break;
            }
            case "HBase Table": {
                JIDP_Main.addTab(Frm, null, new pnl_create_hbase_table());
                JIDP_Main.setSelectedIndex(JIDP_Main.getTabCount() - 1);
                break;
            }
            case "Hive Link": {
                try {
                    JIDP_Main.addTab(Frm, null, new pnl_create_hive_link());
                    JIDP_Main.setSelectedIndex(JIDP_Main.getTabCount() - 1);
                    break;
                } catch (ClassNotFoundException | SQLException | IOException ex) {
                    Logger.getLogger(Frm_Main.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            case "Import to HBase": {
                JIDP_Main.addTab(Frm, null, new pnl_import_vcf());
                JIDP_Main.setSelectedIndex(JIDP_Main.getTabCount() - 1);
                break;
            }
            case "Hive Links": {
            try {
                JIDP_Main.addTab(Frm, null, new pnl_manage_hive_link());
                JIDP_Main.setSelectedIndex(JIDP_Main.getTabCount() - 1);
                break;
            } catch (ClassNotFoundException | SQLException | IOException ex) {
                Logger.getLogger(Frm_Main.class.getName()).log(Level.SEVERE, null, ex);
            }
            }
        }
    }

    private void createStatusBar() {
        // setup status bar
        final LabelStatusBarItem label = new LabelStatusBarItem("Line");
        label.setText("Welcome :");
        label.setAlignment(JLabel.CENTER);
        statusBar1.add(label, JideBoxLayout.FLEXIBLE);
        final LabelStatusBarItem ulabel = new LabelStatusBarItem("Line");
        ulabel.setText("Yu");
        ulabel.setAlignment(JLabel.CENTER);
        ulabel.setForeground(new java.awt.Color(153, 0, 0));
        statusBar1.add(ulabel, JideBoxLayout.FLEXIBLE);
        //     final TimeStatusBarItem time = new TimeStatusBarItem();
        //     statusBar1.add(time, JideBoxLayout.FLEXIBLE);
        final LabelStatusBarItem rclabel = new LabelStatusBarItem("Line");
        // rclabel.setText(getReqCount());
        rclabel.setAlignment(JLabel.CENTER);
        rclabel.setForeground(new java.awt.Color(153, 0, 0));
        statusBar1.add(rclabel, JideBoxLayout.FLEXIBLE);

        final MemoryStatusBarItem gc = new MemoryStatusBarItem();
        statusBar1.add(gc, JideBoxLayout.FLEXIBLE);
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        JIDP_Main = new com.jidesoft.swing.JideTabbedPane();
        statusBar1 = new com.jidesoft.status.StatusBar();
        commandMenuBar1 = new com.jidesoft.action.CommandMenuBar();
        serverMenu = new javax.swing.JMenu();
        Hadoop = new javax.swing.JMenuItem();
        jSeparator1 = new javax.swing.JPopupMenu.Separator();
        HBase = new javax.swing.JMenuItem();
        jSeparator2 = new javax.swing.JPopupMenu.Separator();
        Hive = new javax.swing.JMenuItem();
        menu_create = new javax.swing.JMenu();
        menu_create_hbase_table = new javax.swing.JMenuItem();
        jSeparator8 = new javax.swing.JPopupMenu.Separator();
        menu_create_hive_table = new javax.swing.JMenuItem();
        menu_import = new javax.swing.JMenu();
        menu_import_hbase = new javax.swing.JMenuItem();
        queryMenu = new javax.swing.JMenu();
        HiveQuery = new javax.swing.JMenuItem();
        tableMenu = new javax.swing.JMenu();
        HBaseTable = new javax.swing.JMenuItem();
        jSeparator3 = new javax.swing.JPopupMenu.Separator();
        HiveLinks = new javax.swing.JMenuItem();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowOpened(java.awt.event.WindowEvent evt) {
                formWindowOpened(evt);
            }
        });

        JIDP_Main.setShowCloseButton(true);
        JIDP_Main.setShowCloseButtonOnMouseOver(true);
        JIDP_Main.setShowCloseButtonOnSelectedTab(true);
        JIDP_Main.setShowCloseButtonOnTab(true);

        statusBar1.setBackground(new java.awt.Color(153, 153, 153));

        commandMenuBar1.setStretch(true);

        serverMenu.setText("Server");

        Hadoop.setText("Hadoop");
        Hadoop.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                HadoopActionPerformed(evt);
            }
        });
        serverMenu.add(Hadoop);
        serverMenu.add(jSeparator1);

        HBase.setText("HBase");
        HBase.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                HBaseActionPerformed(evt);
            }
        });
        serverMenu.add(HBase);
        serverMenu.add(jSeparator2);

        Hive.setText("Hive");
        Hive.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                HiveActionPerformed(evt);
            }
        });
        serverMenu.add(Hive);

        commandMenuBar1.add(serverMenu);

        menu_create.setText("Create");

        menu_create_hbase_table.setText("HBase Table");
        menu_create_hbase_table.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menu_create_hbase_tableActionPerformed(evt);
            }
        });
        menu_create.add(menu_create_hbase_table);
        menu_create.add(jSeparator8);

        menu_create_hive_table.setText("Hive Link");
        menu_create_hive_table.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menu_create_hive_tableActionPerformed(evt);
            }
        });
        menu_create.add(menu_create_hive_table);

        commandMenuBar1.add(menu_create);

        menu_import.setText("Import");

        menu_import_hbase.setText("Import to HBase");
        menu_import_hbase.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menu_import_hbaseActionPerformed(evt);
            }
        });
        menu_import.add(menu_import_hbase);

        commandMenuBar1.add(menu_import);

        queryMenu.setText("Query");

        HiveQuery.setText("HiveQuery");
        HiveQuery.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                HiveQueryActionPerformed(evt);
            }
        });
        queryMenu.add(HiveQuery);

        commandMenuBar1.add(queryMenu);

        tableMenu.setText("Manage");

        HBaseTable.setText("HBase Tables");
        HBaseTable.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                HBaseTableActionPerformed(evt);
            }
        });
        tableMenu.add(HBaseTable);
        tableMenu.add(jSeparator3);

        HiveLinks.setText("Hive Links");
        HiveLinks.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                HiveLinksActionPerformed(evt);
            }
        });
        tableMenu.add(HiveLinks);

        commandMenuBar1.add(tableMenu);

        setJMenuBar(commandMenuBar1);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(JIDP_Main, javax.swing.GroupLayout.DEFAULT_SIZE, 502, Short.MAX_VALUE)
            .addComponent(statusBar1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(JIDP_Main, javax.swing.GroupLayout.DEFAULT_SIZE, 233, Short.MAX_VALUE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(statusBar1, javax.swing.GroupLayout.PREFERRED_SIZE, 30, javax.swing.GroupLayout.PREFERRED_SIZE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void formWindowOpened(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_formWindowOpened
        // TODO add your handling code here:
        Insets scnMax = Toolkit.getDefaultToolkit().getScreenInsets(getGraphicsConfiguration());
        int taskBarSize = scnMax.bottom;
        //Toolkit tk = Toolkit.getDefaultToolkit();
        //Dimension d = tk.getScreenSize();
        //this.setSize(d);
        // this.setLocation(0, 0);
        this.setPreferredSize(new Dimension(Toolkit.getDefaultToolkit().getScreenSize().width, Toolkit.getDefaultToolkit().getScreenSize().height - taskBarSize));
    }//GEN-LAST:event_formWindowOpened

    private void HadoopActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_HadoopActionPerformed
        // TODO add your handling code here:
        ShowFrmI(evt);
    }//GEN-LAST:event_HadoopActionPerformed

    private void HBaseActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_HBaseActionPerformed
        // TODO add your handling code here:
        ShowFrmI(evt);
    }//GEN-LAST:event_HBaseActionPerformed

    private void HiveActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_HiveActionPerformed
        // TODO add your handling code here:
        ShowFrmI(evt);
    }//GEN-LAST:event_HiveActionPerformed

    private void HBaseTableActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_HBaseTableActionPerformed
        // TODO add your handling code here:
        ShowFrmI(evt);
    }//GEN-LAST:event_HBaseTableActionPerformed

    private void HiveQueryActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_HiveQueryActionPerformed
        // TODO add your handling code here:
        ShowFrmI(evt);
    }//GEN-LAST:event_HiveQueryActionPerformed

    private void menu_create_hbase_tableActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menu_create_hbase_tableActionPerformed
        // TODO add your handling code here:
        ShowFrmI(evt);
    }//GEN-LAST:event_menu_create_hbase_tableActionPerformed

    private void menu_create_hive_tableActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menu_create_hive_tableActionPerformed
        // TODO add your handling code here:
        ShowFrmI(evt);
    }//GEN-LAST:event_menu_create_hive_tableActionPerformed

    private void menu_import_hbaseActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menu_import_hbaseActionPerformed
        // TODO add your handling code here:
        ShowFrmI(evt);
    }//GEN-LAST:event_menu_import_hbaseActionPerformed

    private void HiveLinksActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_HiveLinksActionPerformed
        // TODO add your handling code here:
        ShowFrmI(evt);
    }//GEN-LAST:event_HiveLinksActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */

        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(Frm_Main.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() {

                new Frm_Main().setVisible(true);
            }
        });
    }
    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JMenuItem HBase;
    private javax.swing.JMenuItem HBaseTable;
    private javax.swing.JMenuItem Hadoop;
    private javax.swing.JMenuItem Hive;
    private javax.swing.JMenuItem HiveLinks;
    private javax.swing.JMenuItem HiveQuery;
    public com.jidesoft.swing.JideTabbedPane JIDP_Main;
    private com.jidesoft.action.CommandMenuBar commandMenuBar1;
    private javax.swing.JPopupMenu.Separator jSeparator1;
    private javax.swing.JPopupMenu.Separator jSeparator2;
    private javax.swing.JPopupMenu.Separator jSeparator3;
    private javax.swing.JPopupMenu.Separator jSeparator8;
    private javax.swing.JMenu menu_create;
    private javax.swing.JMenuItem menu_create_hbase_table;
    private javax.swing.JMenuItem menu_create_hive_table;
    private javax.swing.JMenu menu_import;
    private javax.swing.JMenuItem menu_import_hbase;
    private javax.swing.JMenu queryMenu;
    private javax.swing.JMenu serverMenu;
    private com.jidesoft.status.StatusBar statusBar1;
    private javax.swing.JMenu tableMenu;
    // End of variables declaration//GEN-END:variables
}
