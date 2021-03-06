/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3;

import dbPackage.javaConnection;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.AbstractListModel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import static javax.swing.JOptionPane.PLAIN_MESSAGE;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author pooja.ranawade
 */
public class reviews extends javax.swing.JFrame {

    Connection conn = javaConnection.connectionDB();
    PreparedStatement pst = null;
    ResultSet rs = null;
    JList res_list = new JList();
    DefaultTableModel tableModel = new DefaultTableModel();
    JTable table;
    String businessName;
    ArrayList<String> review_list;

    /**
     * Creates new form reviews
     */
    public reviews(String business) {
        initComponents();
//        reviewPanel.setVisible(false);
        businessName = business;
        nameBusiness.setText(business);

        review_list = review_search(business);

        JOptionPane.showMessageDialog(rootPane, "Showing reviews for: " + business);

        table = new JTable(tableModel);
        JScrollPane res_scp = new JScrollPane(table);
        res_scp.setSize(700, 500);
        res_scp.setLocation(5, 10);
        res_scp.revalidate();
        reviewPanel.add(res_scp);
    }

    private ArrayList<String> review_search(String business) {
        ArrayList<String> listModel = new ArrayList<>();
        try {
            //To change body of generated methods, choose Tools | Templates.
            String sqlQuery = "select REVIEWS.REVIEW_ID,YELP_USER.USER_NAME,REVIEWS.STARS "
                    + "from REVIEWS,BUSINESS,YELP_USER where REVIEWS.BID=BUSINESS.BID and REVIEWS.USER_ID=YELP_USER.USER_ID "
                    + "and BUSINESS.B_NAME='" + business + "'";
            pst = conn.prepareStatement(sqlQuery);
            rs = pst.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();

            //Names of columns
            Vector<String> colNames = new Vector<>();
            int colCount = metaData.getColumnCount();
            for (int i = 2; i <= colCount; i++) {
                colNames.add(metaData.getColumnName(i));
            }

            //data of table
            Vector<Vector<Object>> data = new Vector<>();
            while (rs.next()) {
                Vector<Object> vector = new Vector<>();
                for (int i = 2; i <= colCount; i++) {

                    vector.add(rs.getObject(i));
                    listModel.add(rs.getString("REVIEW_ID"));
                }
                data.add(vector);
            }
            tableModel.setDataVector(data, colNames);

        } catch (SQLException ex) {
            Logger.getLogger(reviews.class.getName()).log(Level.SEVERE, null, ex);
        }
        return listModel;
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
        jPanel2 = new javax.swing.JPanel();
        nameBusiness = new javax.swing.JLabel();
        jLabel1 = new javax.swing.JLabel();
        reviewPanel = new javax.swing.JPanel();
        jButton1 = new javax.swing.JButton();

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 100, Short.MAX_VALUE)
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 100, Short.MAX_VALUE)
        );

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        nameBusiness.setFont(new java.awt.Font("Tahoma", 3, 24)); // NOI18N
        nameBusiness.setText("NameOfBusiness");

        jLabel1.setFont(new java.awt.Font("Tahoma", 3, 24)); // NOI18N
        jLabel1.setText("List of Reviews");

        reviewPanel.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));

        jButton1.setText("Show Review");
        jButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout reviewPanelLayout = new javax.swing.GroupLayout(reviewPanel);
        reviewPanel.setLayout(reviewPanelLayout);
        reviewPanelLayout.setHorizontalGroup(
            reviewPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(reviewPanelLayout.createSequentialGroup()
                .addGap(238, 238, 238)
                .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 126, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(330, Short.MAX_VALUE))
        );
        reviewPanelLayout.setVerticalGroup(
            reviewPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, reviewPanelLayout.createSequentialGroup()
                .addContainerGap(529, Short.MAX_VALUE)
                .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 39, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap())
        );

        javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
        jPanel2.setLayout(jPanel2Layout);
        jPanel2Layout.setHorizontalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addGap(106, 106, 106)
                .addComponent(reviewPanel, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addGap(158, 158, 158)
                .addComponent(jLabel1, javax.swing.GroupLayout.PREFERRED_SIZE, 235, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(18, 18, 18)
                .addComponent(nameBusiness, javax.swing.GroupLayout.DEFAULT_SIZE, 634, Short.MAX_VALUE)
                .addContainerGap())
        );
        jPanel2Layout.setVerticalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addContainerGap(18, Short.MAX_VALUE)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel1)
                    .addComponent(nameBusiness))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(reviewPanel, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap())
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(jPanel2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap())
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
        // TODO add your handling code here:
        int business_int = table.getSelectedRow();
        ArrayList<String> listModel = new ArrayList<>();
        String sqlQuery = "select REVIEWS.R_TEXT from REVIEWS where REVIEWS.REVIEW_ID='" + review_list.get(business_int) + "'";
        System.out.println("reviews.jButton1ActionPerformed()" + sqlQuery);
        try {
            pst = conn.prepareStatement(sqlQuery);
            rs = pst.executeQuery();
            rs.next();
            Clob clob = rs.getClob(1);
            String wholeClob = clob.getSubString(1, (int) clob.length());
            listModel.add(wholeClob);
            res_list.setModel(new AbstractListModel() {
                String[] values = (String[]) listModel.toArray(new String[listModel.size()]);

                @Override
                public int getSize() {
                    return values.length;
                }

                @Override
                public Object getElementAt(int index) {
                    return values[index];
                }
            });
            JOptionPane.showMessageDialog(
                    rootPane,
                    "<html><body><p style='width: 350px;'>" + listModel.get(0) + "</p></body></html>",
                    "REVIEW TEXT", PLAIN_MESSAGE);

        } catch (SQLException ex) {
            Logger.getLogger(reviews.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_jButton1ActionPerformed


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton jButton1;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JLabel nameBusiness;
    private javax.swing.JPanel reviewPanel;
    // End of variables declaration//GEN-END:variables

}
