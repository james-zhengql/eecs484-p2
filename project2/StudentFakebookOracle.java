package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month, descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); //   it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); //   it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        FirstNameInfo info = new FirstNameInfo();
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
            ResultSet rst = stmt.executeQuery(
                    "SELECT DISTINCT First_Name " + 
                            "FROM " + UsersTable + " " +
                            "WHERE LENGTH(First_Name) = (SELECT MAX(LENGTH(First_Name)) FROM " + UsersTable + ") " +
                            "ORDER BY First_Name ASC");

            while (rst.next()) {
                info.addLongName(rst.getString(1));
            }
            rst = stmt.executeQuery(
                    "SELECT DISTINCT First_Name " +
                            "FROM " + UsersTable + " " +
                            "WHERE LENGTH(First_Name) = (SELECT MIN(LENGTH(First_Name)) FROM " + UsersTable + ") " +
                            "ORDER BY First_Name ASC");

            while (rst.next()) {
                info.addShortName(rst.getString(1));
            }
            rst = stmt.executeQuery(
                    "SELECT First_Name, COUNT(*) AS count " +
                            "FROM " + UsersTable + " " +
                            "GROUP BY First_Name " +
                            "ORDER BY count DESC, First_Name ASC");
            if (rst.next()) {
                long count = rst.getLong(2);
                info.setCommonNameCount(count);
                do {
                    info.addCommonName(rst.getString(1));
                } while (rst.next() && rst.getLong(2) == count);
            }
            rst.close();
            stmt.close();
            return info;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery(
                    "SELECT U.User_ID, U.First_Name, U.Last_Name " +
                            "FROM " + UsersTable + " U " +
                            "WHERE NOT EXISTS (" +
                            "    SELECT 1 FROM " + FriendsTable + " F " +
                            "    WHERE F.User1_ID = U.User_ID OR F.User2_ID = U.User_ID" +
                            ") " +
                            "ORDER BY U.User_ID ASC");
            while (rst.next()) {
                results.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }
            rst.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return results;
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery(
                    "SELECT U.User_ID, U.First_Name, U.Last_Name " +
                            "FROM " + UsersTable + " U " +
                            "JOIN " + CurrentCitiesTable + " CC ON CC.User_ID = U.User_ID " +
                            "JOIN " + HometownCitiesTable + " HC ON HC.User_ID = U.User_ID " +
                            "AND CC.Current_City_ID <> HC.Hometown_City_ID " +
                            "ORDER BY U.User_ID ASC");

            while (rst.next()) {
                results.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }
            rst.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return results;
    }

    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
             ResultSet rst = stmt.executeQuery(
                    "SELECT P.Photo_ID, P.Album_ID, P.Photo_Link, A.Album_Name, COUNT(DISTINCT T.Tag_Subject_ID) AS tag_count " +
                            "FROM " + PhotosTable + " P, " + TagsTable + " T, " + AlbumsTable + " A " +
                            "WHERE P.Photo_ID = T.Tag_Photo_ID " +
                            "AND P.Album_ID = A.Album_ID " +
                            "GROUP BY P.Photo_ID, P.Album_ID, P.Photo_Link, A.Album_Name " +
                            "ORDER BY tag_count DESC, P.Photo_ID ASC " +
                            "FETCH FIRST " + num + " ROWS ONLY");
            while (rst.next()) {
                long photoID = rst.getLong(1);
                long albumID = rst.getLong(2);
                String link = rst.getString(3);
                String albumName = rst.getString(4);
                PhotoInfo photo = new PhotoInfo(photoID, albumID, link, albumName);
                TaggedPhotoInfo taggedPhotoInfo = new TaggedPhotoInfo(photo);
                try (Statement userStmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
                    ResultSet userRST = userStmt.executeQuery(
                            "SELECT U.User_ID, U.First_Name, U.Last_Name " +
                                    "FROM " + UsersTable + " U " +
                                    "JOIN " + TagsTable + " T ON U.User_ID = T.Tag_Subject_ID " +
                                    "WHERE T.Tag_Photo_ID = " + photoID + " " +
                                    "ORDER BY U.User_ID ASC");

                    while (userRST.next()) {
                        long userID = userRST.getLong(1);
                        String firstName = userRST.getString(2);
                        String lastName = userRST.getString(3);
                        taggedPhotoInfo.addTaggedUser(new UserInfo(userID, firstName, lastName));
                    }
                    userRST.close();
                } catch (SQLException e) {
                    System.err.println("Error fetching tagged users: " + e.getMessage());
                }
                results.add(taggedPhotoInfo);
            }
            rst.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return results;
    }

    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
            ResultSet rst = stmt.executeQuery(
                    "SELECT U1.User_ID AS U1ser_ID, U1.First_Name AS User1_First, U1.Last_Name AS User1_Last, U1.Year_of_Birth AS User1_Birth, " +
                            "U2.User_ID AS U2ser_ID, U2.First_Name AS User2_First, U2.Last_Name AS User2_Last, U2.Year_of_Birth AS User2_Birth, " +
                            "COUNT(T1.Tag_Photo_ID) AS shared_photos " +
                            "FROM " + UsersTable + " U1 " +
                            "JOIN " + TagsTable + " T1 ON U1.User_ID = T1.Tag_Subject_ID " +
                            "JOIN " + TagsTable + " T2 ON T1.Tag_Photo_ID = T2.Tag_Photo_ID " +
                            "JOIN " + UsersTable + " U2 ON U2.User_ID = T2.Tag_Subject_ID AND U1.User_ID < U2.User_ID " +
                            "LEFT JOIN " + FriendsTable + " F ON U1.User_ID = F.User1_ID AND U2.User_ID = F.User2_ID " +
                            "WHERE U1.Gender = U2.Gender " +
                            "AND ABS(U1.Year_of_Birth - U2.Year_of_Birth) <= " + yearDiff + " " +
                            "AND F.User1_ID IS NULL " +
                            "GROUP BY U1.User_ID, U1.First_Name, U1.Last_Name, U1.Year_of_Birth, " +
                            "U2.User_ID, U2.First_Name, U2.Last_Name, U2.Year_of_Birth " +
                            "ORDER BY shared_photos DESC, U1ser_ID ASC, U2ser_ID ASC " +
                            "FETCH FIRST " + num + " ROWS ONLY");

            while (rst.next()) {
                long user1ID = rst.getLong(1);
                String user1FirstName = rst.getString(2);
                String user1LastName = rst.getString(3);
                long user1Year = rst.getLong(4);
                long user2ID = rst.getLong(5);
                String user2FirstName = rst.getString(6);
                String user2LastName = rst.getString(7);
                long user2Year = rst.getLong(8);
                UserInfo user1 = new UserInfo(user1ID, user1FirstName, user1LastName);
                UserInfo user2 = new UserInfo(user2ID, user2FirstName, user2LastName);
                MatchPair matchPair = new MatchPair(user1, user1Year, user2, user2Year);
                try (Statement photoStmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
                    ResultSet photoRST = photoStmt.executeQuery(
                            "SELECT P.Photo_ID, A.Album_ID, P.Photo_Link, A.Album_Name " +
                                    "FROM " + TagsTable + " T1 " +
                                    "JOIN " + TagsTable + " T2 ON T1.Tag_Photo_ID = T2.Tag_Photo_ID " +
                                    "JOIN " + PhotosTable + " P ON T1.Tag_Photo_ID = P.Photo_ID " +
                                    "JOIN " + AlbumsTable + " A ON P.Album_ID = A.Album_ID " +
                                    "WHERE T1.Tag_Subject_ID = " + user1ID + " AND T2.Tag_Subject_ID = " + user2ID + " " +
                                    "ORDER BY P.Photo_ID ASC");

                    while (photoRST.next()) {
                        PhotoInfo photo = new PhotoInfo(photoRST.getLong(1), photoRST.getLong(2), photoRST.getString(3), photoRST.getString(4));
                        matchPair.addSharedPhoto(photo);
                    }

                    photoRST.close();
                } catch (SQLException e) {
                    System.err.println("Error fetching shared photos: " + e.getMessage());
                }

                results.add(matchPair);
            }
            rst.close(); 
            stmt.close();
        } catch (SQLException e) {
            System.err.println("Error fetching match results: " + e.getMessage());
        }

        return results;
    }


    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
  
            stmt.executeUpdate("CREATE VIEW Bi_Friend AS "+
                                    "SELECT F.user1_id, F.user2_id "+
                                        "FROM "+FriendsTable+" F "+
                                    "UNION "+
                                    "SELECT F1.user2_id, F1.user1_id "+
                                        "FROM "+FriendsTable+" F1"
                                );

            ResultSet rst = stmt.executeQuery("SELECT user1_id,user2_id "+
                                "FROM ( "+
                                "SELECT user1_id, user2_id "+
                                "FROM "+
                                    "( SELECT B1.user1_id AS user1_id, B2.user2_id AS user2_id "+
                                        "FROM Bi_Friend B1, Bi_Friend B2 "+
                                        "WHERE B1.user1_id < B2.user2_id "+
                                        "AND B1.user2_id = B2.user1_id "+
                                        "AND NOT EXISTS ("+
                                        "SELECT 1 "+
                                        "FROM "+FriendsTable+" F "+ 
                                        "WHERE F.user1_id = B1.user1_id "+
                                        "AND F.user2_id = B2.user2_id)  "+
                                ") GROUP BY user1_id, user2_id "+
                                "ORDER BY COUNT(*) DESC, user1_id, user2_id )"+
                                "WHERE ROWNUM <= " + num 
                                );

            while (rst.next()) {
                long mutual1_friendID = -1;
                String mutual1_firstName = "ERROR";
                String mutual1_lastName = "ERROR";
                long mutual2_friendID = -1;
                String mutual2_firstName = "ERROR";
                String mutual2_lastName = "ERROR";   
                try (Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                    ResultSet rst1 = stmt2.executeQuery(
                            "SELECT U.user_ID, U.first_name, U.last_name " +
                            "FROM " + UsersTable + " U " +
                            "WHERE U.user_id = " + rst.getLong(1)
                    );
                    if (rst1.next()) {
                        mutual1_friendID = rst1.getLong(1);
                        mutual1_firstName = rst1.getString(2);
                        mutual1_lastName = rst1.getString(3);
                    }
                    rst1.close();
                    

                    UserInfo user1 = new UserInfo(mutual1_friendID, mutual1_firstName, mutual1_lastName);


                    ResultSet rst2 = stmt2.executeQuery(
                            "SELECT U.user_ID, U.first_name, U.last_name " +
                            "FROM " + UsersTable + " U " +
                            "WHERE U.user_id = " + rst.getLong(2)
                    );
                    if (rst2.next()) {
                        mutual2_friendID = rst2.getLong(1);
                        mutual2_firstName = rst2.getString(2);
                        mutual2_lastName = rst2.getString(3);
                    }
                    rst2.close();


                    UserInfo user2 = new UserInfo(mutual2_friendID, mutual2_firstName, mutual2_lastName);
                    UsersPair info = new UsersPair(user1, user2);


                    ResultSet rst3 = stmt2.executeQuery(
                            "SELECT U.user_id, U.first_name, U.last_name " +
                            "FROM Bi_Friend B1, Bi_Friend B2, " + UsersTable + " U " +
                            "WHERE U.user_id = B1.user2_id AND B1.user2_id = B2.user1_id " + 
                            "AND B1.user1_id = " + rst.getLong(1) + " AND B2.user2_id = " + rst.getLong(2) + " " +
                            "ORDER BY U.user_id"
                    );
                    while (rst3.next()) {
                        info.addSharedFriend(new UserInfo(rst3.getLong(1), rst3.getString(2), rst3.getString(3)));
                    }
                    rst3.close();
                    results.add(info);
                }    
            }
            stmt.executeUpdate("DROP VIEW Bi_Friend");
            // rst.close();
            stmt.close();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            ResultSet rst = stmt.executeQuery("SELECT MAX(event_count) FROM "+
            "(SELECT COUNT(*) AS event_count "+
            "FROM "+ CitiesTable + " C, "+ EventsTable + " E " +
            "WHERE C.city_id = E.event_city_id "+
            "GROUP BY C.state_name)");
            long eventCount = 0;
            if(rst.next()){
                eventCount = rst.getLong(1);
            }
            EventStateInfo info = new EventStateInfo(eventCount);
            rst = stmt.executeQuery("SELECT DISTINCT C.state_name "+
                                "FROM "+ CitiesTable +" C, "+ EventsTable + " E "+
                                "WHERE C.city_id = E.event_city_id "+
                                "GROUP BY C.state_name "+
                                "HAVING COUNT(*) = " + eventCount );
            while (rst.next()) {
                info.addState(rst.getString(1));
            }
            rst.close();
            stmt.close();
            return info; 
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT u.user_id, u.first_name, u.last_name " +
                "FROM " + UsersTable + " u " +
                "JOIN " + FriendsTable + " F ON (u.user_id = F.user1_id OR u.user_id = F.user2_id) " +
                "WHERE (F.user1_id = " + userID + " OR F.user2_id = " + userID + ") " +
                "ORDER BY u.year_of_birth, u.month_of_birth, u.day_of_birth, u.user_id DESC"
            );

            long youngest_friendID = -1;
            String youngest_firstName = "ERROR";
            String youngest_lastName = "ERROR";
            long oldest_friendID = -1;
            String oldest_firstName = "ERROR";
            String oldest_lastName = "ERROR";            
            while(rst.next()){
                if(rst.isLast()){
                    youngest_friendID = rst.getLong(1);
                    youngest_firstName = rst.getString(2);
                    youngest_lastName = rst.getString(3);
                }
                if(rst.isFirst()){
                    oldest_friendID = rst.getLong(1);
                    oldest_firstName = rst.getString(2);
                    oldest_lastName = rst.getString(3);
                }
                
            }

            return new AgeInfo(new UserInfo(oldest_friendID,oldest_firstName,oldest_lastName),new UserInfo(youngest_friendID,youngest_firstName,youngest_lastName)); 

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
            ResultSet rst = stmt.executeQuery("SELECT DISTINCT u1.user_id AS user1_id, u1.first_name AS user1_first_name, u1.last_name AS user1_last_name, u2.user_id AS user2_id,u2.first_name AS user2_first_name,u2.last_name AS user2_last_name "+
                                            "FROM "+ UsersTable + " u1, " + UsersTable + " u2, " + FriendsTable + " F, "+ HometownCitiesTable + " H1, " + HometownCitiesTable + " H2 "+
                                            "WHERE u1.last_name = u2.last_name AND "+
                                            "u1.user_id = H1.user_id AND u2.user_id = H2.user_id AND H1.hometown_city_id = H2.hometown_city_id "+
                                            "AND u1.user_id < u2.user_id AND u1.user_id = F.user1_id AND u2.user_id = F.user2_id "+
                                            "AND ABS(u1.year_of_birth-u2.year_of_birth) < 10 "+
                                            "ORDER BY user1_id,user2_id");
            while(rst.next()){
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2),  rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(4), rst.getString(5),  rst.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
