package myDB;

import java.io.BufferedReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import static myDB.dbConnection.connectionDB;
import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author pooja.ranawade
 */
public class populate_user {

    public static void main(String[] args) throws SQLException {
        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;
        PreparedStatement preparedStatement2 = null;
        PreparedStatement preparedStatement3 = null;

        String sqlquery = "INSERT INTO YELP_USER"
                + "(yelping_since, funny_votes, useful_votes, cool_votes, review_count, user_name, user_id, fans, average_stars, u_type, hot_compliment, more_compliment, profile_compliment, cute_compliment, list_compliment, note_compliment, plain_compliment, cool_compliment, funny_compliment, writer_compliment, photos_compliment ) VALUES"
                + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String sqlquery2 = "INSERT INTO FRIENDS (USER_ID, FRIEND_ID) "
                + "VALUES (?, ?)";
        String sqlquery3 = "INSERT INTO ELITE_YEARS" + "(USER_ID, ELITE) VALUES" + "(?,?)";

        JSONParser parser = new JSONParser();
        String file = "src\\YelpDataset\\yelp_user.json";
        try {
            dbConnection = connectionDB();
            preparedStatement = dbConnection.prepareStatement(sqlquery);
            preparedStatement2 = dbConnection.prepareStatement(sqlquery2);
            preparedStatement3 = dbConnection.prepareStatement(sqlquery3);
            try (FileReader fileReader = new FileReader(file)) {
                System.out.println("file opened");
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    Object obj = parser.parse(line);
                    JSONObject jsonObject = (JSONObject) obj;
                    String yelping_since = (String) jsonObject.get("yelping_since");
                    preparedStatement.setString(1, yelping_since);

                    int review_count = ((Long) jsonObject.get("review_count")).intValue();
                    preparedStatement.setInt(5, review_count);

                    String name = (String) jsonObject.get("name");
                    preparedStatement.setString((6), name);

                    String user_id = (String) jsonObject.get("user_id");
                    preparedStatement.setString(7, user_id);

                    int fans = ((Long) jsonObject.get("fans")).intValue();
                    preparedStatement.setInt(8, fans);

                    float avg_stars = ((Double) jsonObject.get("average_stars")).floatValue();
                    preparedStatement.setFloat(9, avg_stars);

                    String type = (String) jsonObject.get("type");
                    preparedStatement.setString(10, type);

                    JSONObject votes = (JSONObject) jsonObject.get("votes");
                    int funny_votes = ((Long) votes.get("funny")).intValue();
                    int useful_votes = ((Long) votes.get("useful")).intValue();
                    int cool_votes = ((Long) votes.get("cool")).intValue();

                    preparedStatement.setInt(2, funny_votes);
                    preparedStatement.setInt(3, useful_votes);
                    preparedStatement.setInt(4, cool_votes);

                    JSONObject compliments = (JSONObject) jsonObject.get("compliments");

                    int hotCompliment;
                    int moreCompliment;
                    int profileCompliment;
                    int cuteCompliment;
                    int listCompliment;
                    int noteCompliment;
                    int plainCompliment;
                    int coolCompliment;
                    int funnyCompliment;
                    int writerCompliment;
                    int photosCompliment;
                    if (compliments.get("hot") != null) {
                        hotCompliment = ((Long) compliments.get("hot")).intValue();
                    } else {
                        hotCompliment = 0;
                    }

                    if (compliments.get("more") != null) {
                        moreCompliment = ((Long) compliments.get("more")).intValue();
                    } else {
                        moreCompliment = 0;
                    }

                    if (compliments.get("profile") != null) {
                        profileCompliment = ((Long) compliments.get("profile")).intValue();
                    } else {
                        profileCompliment = 0;
                    }

                    if (compliments.get("cute") != null) {
                        cuteCompliment = ((Long) compliments.get("cute")).intValue();
                    } else {
                        cuteCompliment = 0;
                    }

                    if (compliments.get("list") != null) {
                        listCompliment = ((Long) compliments.get("list")).intValue();
                    } else {
                        listCompliment = 0;
                    }

                    if (compliments.get("note") != null) {
                        noteCompliment = ((Long) compliments.get("note")).intValue();
                    } else {
                        noteCompliment = 0;
                    }

                    if (compliments.get("plain") != null) {
                        plainCompliment = ((Long) compliments.get("plain")).intValue();
                    } else {
                        plainCompliment = 0;
                    }

                    if (compliments.get("cool") != null) {
                        coolCompliment = ((Long) compliments.get("cool")).intValue();
                    } else {
                        coolCompliment = 0;
                    }

                    if (compliments.get("funny") != null) {
                        funnyCompliment = ((Long) compliments.get("funny")).intValue();
                    } else {
                        funnyCompliment = 0;
                    }

                    if (compliments.get("writer") != null) {
                        writerCompliment = ((Long) compliments.get("writer")).intValue();
                    } else {
                        writerCompliment = 0;
                    }

                    if (compliments.get("photos") != null) {
                        photosCompliment = ((Long) compliments.get("photos")).intValue();
                    } else {
                        photosCompliment = 0;
                    }

                    preparedStatement.setInt(11, hotCompliment);
                    preparedStatement.setInt(12, moreCompliment);
                    preparedStatement.setInt(13, profileCompliment);
                    preparedStatement.setInt(14, cuteCompliment);
                    preparedStatement.setInt(15, listCompliment);
                    preparedStatement.setInt(16, noteCompliment);
                    preparedStatement.setInt(17, plainCompliment);
                    preparedStatement.setInt(18, coolCompliment);
                    preparedStatement.setInt(19, funnyCompliment);
                    preparedStatement.setInt(20, writerCompliment);
                    preparedStatement.setInt(21, photosCompliment);

                    preparedStatement.executeUpdate();

                    if (jsonObject.get("friends") != null) {
                        JSONArray friendarray = (JSONArray) jsonObject.get("friends");
                        Iterator<String> iterator = friendarray.iterator();
                        String friend_id;

                        while (iterator.hasNext()) {
                            friend_id = iterator.next();
                            preparedStatement2.setString(1, user_id);
                            preparedStatement2.setString(2, friend_id);
                            preparedStatement2.executeUpdate();
                        }
                    }

                    if (jsonObject.get("elite") != null) {
                        JSONArray elitearray = (JSONArray) jsonObject.get("elite");
                        Iterator<Long> iterator2 = elitearray.iterator();
                        int elite_year;

                        while (iterator2.hasNext()) {
                            elite_year = (iterator2.next()).intValue();
                            preparedStatement3.setString(1, user_id);
                            preparedStatement3.setInt(2, elite_year);
                            preparedStatement3.executeUpdate();
                        }
                    }
                }
                fileReader.close();
            } catch (SQLException | IOException | ParseException e) {
                System.out.println(e.toString());
            } finally {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (preparedStatement2 != null) {
                    preparedStatement2.close();
                }
                if (preparedStatement3 != null) {
                    preparedStatement3.close();
                }
                if (dbConnection != null) {
                    dbConnection.close();
                }
            }

        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }
}
