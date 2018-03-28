package myDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.swing.JOptionPane;

/**
 *
 * @author pooja.ranawade
 */
public class dbConnection {

    public static Connection connectionDB() {

        Connection con = null;
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (Exception e) {
            System.out.println("driver" + e.toString());
        }
        try {
            con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:admin", "system", "admin");
            JOptionPane.showMessageDialog(null, "SUCCESS!!");
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(null, e);
        }
        return con;
    }

}
