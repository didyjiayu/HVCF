/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Server;

import importToNewTable.createDataJob;
import java.awt.Cursor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author yujia1986
 */
public class pnl_hive_server extends javax.swing.JPanel {

    /**
     * Creates new form pnl_hive_server
     */
    public pnl_hive_server() {
        initComponents();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        Pnl_back = new javax.swing.JPanel();
        jScrollPane1 = new javax.swing.JScrollPane();
        Txt_command = new javax.swing.JTextArea();
        hiveserver2 = new javax.swing.JButton();
        hiveUserField = new javax.swing.JTextField();
        hiveUserPassword = new javax.swing.JLabel();
        hiveServerPort = new javax.swing.JLabel();
        hiveServerPortField = new javax.swing.JTextField();
        hiveServer = new javax.swing.JLabel();
        hiveUser = new javax.swing.JLabel();
        hiveServerField = new javax.swing.JTextField();
        hiveUserPasswordField = new javax.swing.JPasswordField();

        setEnabled(false);

        Txt_command.setEditable(false);
        Txt_command.setColumns(20);
        Txt_command.setRows(5);
        jScrollPane1.setViewportView(Txt_command);

        hiveserver2.setText("Hiveserver2");
        hiveserver2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                hiveserver2ActionPerformed(evt);
            }
        });

        hiveUserPassword.setText("Password");

        hiveServerPort.setText("Port");

        hiveServer.setText("Hive Server");

        hiveUser.setText("User");

        javax.swing.GroupLayout Pnl_backLayout = new javax.swing.GroupLayout(Pnl_back);
        Pnl_back.setLayout(Pnl_backLayout);
        Pnl_backLayout.setHorizontalGroup(
            Pnl_backLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jScrollPane1, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 698, Short.MAX_VALUE)
            .addGroup(Pnl_backLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(Pnl_backLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(Pnl_backLayout.createSequentialGroup()
                        .addComponent(hiveServer)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(hiveServerField)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(hiveServerPort)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(hiveServerPortField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(hiveUser)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(hiveUserField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(hiveUserPassword)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(hiveUserPasswordField, javax.swing.GroupLayout.PREFERRED_SIZE, 57, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(Pnl_backLayout.createSequentialGroup()
                        .addComponent(hiveserver2)
                        .addGap(0, 0, Short.MAX_VALUE)))
                .addContainerGap())
        );

        Pnl_backLayout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {hiveServerPortField, hiveUserField, hiveUserPasswordField});

        Pnl_backLayout.setVerticalGroup(
            Pnl_backLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, Pnl_backLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(Pnl_backLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(hiveServer)
                    .addComponent(hiveServerField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(hiveServerPort)
                    .addComponent(hiveServerPortField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(hiveUser)
                    .addComponent(hiveUserField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(hiveUserPassword)
                    .addComponent(hiveUserPasswordField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(hiveserver2)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 397, Short.MAX_VALUE))
        );

        Pnl_backLayout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] {hiveServer, hiveServerField, hiveServerPort, hiveServerPortField, hiveUser, hiveUserField, hiveUserPassword, hiveUserPasswordField});

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(Pnl_back, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(Pnl_back, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
    }// </editor-fold>//GEN-END:initComponents

    private void hiveserver2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_hiveserver2ActionPerformed
        try {
            saveServerInfo();
            hiveserver2.setEnabled(false);
            runhive runabletask = new runhive(0);
            new Thread(runabletask).start();
            runhive runabletask2 = new runhive(1);
            new Thread(runabletask2).start();
        } catch (IOException ex) {
            Logger.getLogger(pnl_hive_server.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_hiveserver2ActionPerformed

    public class runhive implements Runnable {

        private Integer Server_or_Metastore;

        public runhive(Integer Server_or_Metastore) {
            this.Server_or_Metastore = Server_or_Metastore;
        }

        public void run_hive(Integer Server_or_Metastore) {
            Cursor hourglassCursor = new Cursor(Cursor.WAIT_CURSOR);
            Pnl_back.setCursor(hourglassCursor);
            try {
                // TODO add your handling code here:
                String cmd = "";
                if (Server_or_Metastore == 0) {
                    cmd = "hiveserver2";
                } else {
                    cmd = "hive --service metastore";
                }

                Process pr = Runtime.getRuntime().exec(cmd);
                BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    Txt_command.setText(Txt_command.getText() + line + "\n");
                }
                in.close();
                try {
                    pr.waitFor();
                } catch (InterruptedException ex) {
                    Logger.getLogger(pnl_hive_server.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (IOException ex) {
                Logger.getLogger(pnl_hive_server.class.getName()).log(Level.SEVERE, null, ex);
            }
            Cursor normalCursor = new Cursor(Cursor.DEFAULT_CURSOR);
            Pnl_back.setCursor(normalCursor);
        }

        @Override
        public void run() {
            run_hive(Server_or_Metastore);
        }

    }
    
    public void saveServerInfo() throws IOException {
        String[] serverInfo = new String[4];
        serverInfo[0] = hiveServerField.getText();
        serverInfo[1] = hiveServerPortField.getText();
        serverInfo[2] = hiveUserField.getText();
        serverInfo[3] = String.valueOf(hiveUserPasswordField.getPassword());
        importToNewTable.createDataJob pathClear = new createDataJob();
        File path = new File("/tmp/HVCF/hiveServerInfo");
        pathClear.deleteDir(path);
        path.mkdirs();
        File f = new File("/tmp/HVCF/hiveServerInfo/hiveServerInfo.txt");
        try (FileWriter out = new FileWriter(f)) {
            out.write(serverInfo[0]+","+serverInfo[1]+","+serverInfo[2]+","+serverInfo[3]);
            out.close();
        }
    }


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JPanel Pnl_back;
    private javax.swing.JTextArea Txt_command;
    private javax.swing.JLabel hiveServer;
    private javax.swing.JTextField hiveServerField;
    private javax.swing.JLabel hiveServerPort;
    private javax.swing.JTextField hiveServerPortField;
    private javax.swing.JLabel hiveUser;
    private javax.swing.JTextField hiveUserField;
    private javax.swing.JLabel hiveUserPassword;
    private javax.swing.JPasswordField hiveUserPasswordField;
    private javax.swing.JButton hiveserver2;
    private javax.swing.JScrollPane jScrollPane1;
    // End of variables declaration//GEN-END:variables
}
