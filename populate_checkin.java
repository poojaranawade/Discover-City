package myDB;

import static dbPopulate.populate_checkin.convert_day;
import static dbPopulate.populate_checkin.convert_hour;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import static myDB.dbConnection.connectionDB;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author pooja.ranawade
 */
public class populate_checkin {

    public static void main(String[] args) throws SQLException {
        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;
        String sqlquery = "INSERT INTO check_in"
                + "(ci_day, ci_hour, ci_count, bid) VALUES"
                + "(?,?,?,?)";

        JSONParser parser = new JSONParser();
        String file = "src\\YelpDataset\\yelp_checkin.json";
        try {
            dbConnection = connectionDB();
            preparedStatement = dbConnection.prepareStatement(sqlquery);
            try (FileReader fileReader = new FileReader(file)) {
                System.out.println("file opened");
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    Object obj = parser.parse(line);
                    JSONObject jsonObject = (JSONObject) obj;

                    String business_id = (String) jsonObject.get("business_id");
                    preparedStatement.setString(4, business_id);

                    JSONObject jsonObject2 = (JSONObject) jsonObject.get("checkin_info");
                    String ci_day;
                    int ci_hour;
                    int ci_count;

                    for (Object key : jsonObject2.keySet()) {
                        String keyStr = (String) key;
                        Object keyvalue = jsonObject2.get(keyStr);
                        ci_day = convert_day(keyStr);
                        ci_hour = convert_hour(keyStr);
                        ci_count = ((Long) keyvalue).intValue();

                        preparedStatement.setString(1, ci_day);
                        preparedStatement.setInt(2, ci_hour);
                        preparedStatement.setInt(3, ci_count);
                        preparedStatement.executeUpdate();
                    }
                }
                fileReader.close();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (dbConnection != null) {
                dbConnection.close();
            }
        }
    }
}
