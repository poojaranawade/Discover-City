package myDB;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import static myDB.dbConnection.connectionDB;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author pooja.ranawade
 */
public class populate_review {

    public static void main(String[] args) throws SQLException {
        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;
        String sqlquery = "INSERT INTO REVIEWS"
                + "(funny_vote, useful_vote, cool_vote, user_id, review_id, stars, r_date, r_text, r_type, bid) VALUES"
                + "(?,?,?,?,?,?,?,?,?,?)";

        JSONParser parser = new JSONParser();
        try {
            dbConnection = connectionDB();
            preparedStatement = dbConnection.prepareStatement(sqlquery);
            String file = "src\\YelpDataset\\yelp_review.json";
            try (FileReader fileReader = new FileReader(file)) {
                System.out.println("file opened");
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    Object obj = parser.parse(line);
                    JSONObject jsonObject = (JSONObject) obj;

                    JSONObject votes = (JSONObject) jsonObject.get("votes");
                    int funnyVotes, usefulVotes, coolVotes;
                    funnyVotes = ((Long) votes.get("funny")).intValue();
                    usefulVotes = ((Long) votes.get("useful")).intValue();
                    coolVotes = ((Long) votes.get("cool")).intValue();
                    preparedStatement.setInt(1, funnyVotes);
                    preparedStatement.setInt(2, usefulVotes);
                    preparedStatement.setInt(3, coolVotes);

                    String user_id = (String) jsonObject.get("user_id");
                    preparedStatement.setString(4, user_id);

                    String review_id = (String) jsonObject.get("review_id");
                    preparedStatement.setString(5, review_id);

                    int stars = ((Long) jsonObject.get("stars")).intValue();
                    preparedStatement.setInt(6, stars);

                    String date = (String) jsonObject.get("date");
                    preparedStatement.setString(7, date);

                    String text = (String) jsonObject.get("text");
                    preparedStatement.setString(8, text);

                    String type = (String) jsonObject.get("type");
                    preparedStatement.setString(9, type);

                    String business_id = (String) jsonObject.get("business_id");
                    preparedStatement.setString(10, business_id);

                    preparedStatement.executeUpdate();
                }
                fileReader.close();
            }

        } catch (SQLException | IOException | ParseException e) {
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
