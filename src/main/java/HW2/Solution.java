package HW2;

import HW2.business.*;
import HW2.data.DBConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import HW2.data.PostgreSQLErrorCodes;

import java.util.ArrayList;

import static HW2.business.ReturnValue.*;
import static org.junit.Assert.assertEquals;


public class Solution {
    public static void createTables()
    {
        InitialState.createInitialState();
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("CREATE TABLE test\n" +
                    "(\n" +
                    "    test_id integer NOT NULL,\n" +
                    "    semester integer NOT NULL ,\n" +
                    "    time integer NOT NULL,\n" +
                    "    room integer NOT NULL,\n" +
                    "    day integer NOT NULL,\n" +
                    "    credit_points integer NOT NULL,\n" +
                    "    PRIMARY KEY (test_id, semester),\n" +
                    "    CHECK (test_id > 0 AND credit_points > 0 AND room > 0 " +
                    "    AND semester > 0 AND semester < 4 AND time > 0 AND time < 4 " +
                    "    AND day > 0 AND day < 32)" +
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE TABLE student\n" +
                    "(\n" +
                    "    student_id integer NOT NULL,\n" +
                    "    name text NOT NULL ,\n" +
                    "    faculty text NOT NULL,\n" +
                    "    credit_points integer NOT NULL,\n" +
                    "    PRIMARY KEY (student_id),\n" +
                    "    CHECK (student_id > 0 AND credit_points >= 0) \n" +
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE TABLE supervisor\n" +
                    "(\n" +
                    "    supervisor_id integer NOT NULL,\n" +
                    "    name text NOT NULL,\n" +
                    "    salary integer NOT NULL,\n" +
                    "    PRIMARY KEY (supervisor_id),\n" +
                    "    CHECK (supervisor_id > 0 AND salary >= 0) \n" +
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE TABLE attendance\n" +
                    "(\n" +
                    "    student_id integer ,\n" +
                    "    test_id integer ,\n" +
                    "    semester integer ,\n" +
                    /*"    test_credit_points integer NOT NULL,\n" +*/
                    "    PRIMARY KEY (student_id,test_id,semester),\n" +
                    "    FOREIGN KEY (student_id) REFERENCES student(student_id) ON DELETE CASCADE ON UPDATE CASCADE, "+
                    "    FOREIGN KEY (test_id, semester) REFERENCES test(test_id, semester) ON DELETE CASCADE ON UPDATE CASCADE "+
                    /*"    FOREIGN KEY (semester) REFERENCES test(semester) ON DELETE CASCADE ON UPDATE CASCADE"+*/
                    /*"    CHECK (id > 0 AND credit_points >= 0) \n" +*/
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE TABLE overseeing\n" +
                    "(\n" +
                    "    supervisor_id integer ,\n" +
                    "    test_id integer ,\n" +
                    "    semester integer ,\n" +
                    /*"    test_credit_points integer NOT NULL,\n" +*/
                    "    PRIMARY KEY (supervisor_id,test_id,semester),\n" +
                    "    FOREIGN KEY (supervisor_id) REFERENCES supervisor(supervisor_id) ON DELETE CASCADE ON UPDATE CASCADE,\n"+
                    "    FOREIGN KEY (test_id, semester) REFERENCES test(test_id, semester) ON DELETE CASCADE ON UPDATE CASCADE \n"+
                    /*"    FOREIGN KEY (semester) REFERENCES test(semester) ON DELETE CASCADE ON UPDATE CASCADE\n"+*/
                    /*"    CHECK (id > 0 AND credit_points >= 0) \n" +*/
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE OR REPLACE VIEW test_avg_costs AS\n" +
                            "SELECT test_overseeing.test_id, "+
                    "test_overseeing.semester, "+
                    "COALESCE(avg(s.salary), (0)::numeric) AS avg_salary "+
                    "FROM (( SELECT t.test_id, "+
                            "t.semester, "+
                            "o.supervisor_id "+
                            "FROM (test t "+
                           "FULL JOIN overseeing o ON (((t.test_id = o.test_id) AND (t.semester = o.semester))))) test_overseeing "+
                    "FULL JOIN supervisor s ON ((test_overseeing.supervisor_id = s.supervisor_id))) "+
            "GROUP BY test_overseeing.test_id, test_overseeing.semester");
            pstmt.execute();
            pstmt = connection.prepareStatement(" CREATE OR REPLACE VIEW supervisor_salaries AS "+
                    "SELECT s.supervisor_id,\n" +
                    "    sum(s.salary) AS salary\n" +
                    "   FROM (supervisor s\n" +
                    "     FULL JOIN overseeing o ON ((s.supervisor_id = o.supervisor_id)))\n" +
                    "  WHERE (o.test_id IS NOT NULL)\n" +
                    "  GROUP BY s.supervisor_id\n" +
                    "UNION\n" +
                    " SELECT s.supervisor_id,\n" +
                    "    0 AS salary\n" +
                    "   FROM (supervisor s\n" +
                    "     LEFT JOIN overseeing o ON ((s.supervisor_id = o.supervisor_id)))\n" +
                    "  WHERE (o.test_id IS NULL)");
            pstmt.execute();
            /*pstmt = connection.prepareStatement("CREATE OR REPLACE VIEW student_all_tests_points AS "+
                    "SELECT student_id,sum(credit_points) as points\n" +
                    "FROM attendance AS A\n" +
                    "LEFT JOIN test AS T\n" +
                    "ON A.test_id = T.test_id\n" +
                    "AND A.semester = T.semester\n" +
                    "GROUP BY student_id");*/
            pstmt = connection.prepareStatement("CREATE OR REPLACE VIEW student_all_tests_points AS "+
                    "SELECT S.student_id, coalesce(sum(T.credit_points),0) AS points\n" +
                    "FROM student S \n" +
                    "FULL JOIN attendance A\n" +
                    "ON S.student_id = A.student_id \n" +
                    "FULL JOIN test T\n" +
                    "ON T.test_id = A.test_id \n" +
                    "AND T.semester = A.semester\n" +
                    "GROUP BY S.student_id");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE OR REPLACE VIEW student_after_tests_points AS "+
                    "SELECT S.student_id,\n" +
                    "points+credit_points AS points\n" +
                    "from student AS S, student_all_tests_points AS SA\n" +
                    "WHERE S.student_id = SA.student_id ");
            pstmt.execute();
            /*pstmt = connection.prepareStatement("CREATE VIEW Attendance\n" +
                    "(\n" +
                    "SELECT * FROM \n" +
                    "Oerseeing AS O INNER JOIN\n" +
                    "Supervisor AS S\n"+
                    "ON O.supervisorID=S.supervisorID)");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE VIEW supervisors_overseeing\n" +
                    "(\n" +
                            "SELECT * FROM \n" +
                            "Overseeing AS O INNER JOIN\n" +
                            "Supervisor AS S\n"+
                            "ON O.supervisorID=S.supervisorID)");
            pstmt.execute();*/

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

    }

    public static void clearTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM test");
            pstmt.execute();

            pstmt = connection.prepareStatement("DELETE FROM student");
            pstmt.execute();

            pstmt = connection.prepareStatement("DELETE FROM supervisor");
            pstmt.execute();

            pstmt = connection.prepareStatement("DELETE FROM attendance");
            pstmt.execute();

            pstmt = connection.prepareStatement("DELETE FROM overseeing");
            pstmt.execute();
        } catch (SQLException e) {
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static void dropTables() {
        InitialState.dropInitialState();
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS student_after_tests_points");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS student_all_tests_points");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS test_avg_costs");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS supervisor_salaries");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS attendance");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS overseeing");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS supervisor");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS student");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS test");
            pstmt.execute();


        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static ReturnValue addTest(Test test) {
        /*
        Input: Test to be added.
        Output: ReturnValue with the following conditions:
        * OK in case of success
        * BAD_PARAMS in case of illegal parameters.
        * ALREADY_EXISTS if a test with the same ID and Semester already exists
        * ERROR in case of a database error
       */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO test" +
                    " VALUES (?, ?, ?, ?, ?, ?)");
            pstmt.setInt(1,test.getId());
            pstmt.setInt(2, test.getSemester());
            pstmt.setInt(3,test.getTime());
            pstmt.setInt(4,test.getRoom());
            pstmt.setInt(5,test.getDay());
            pstmt.setInt(6,test.getCreditPoints());

            pstmt.execute();
            return OK;

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ReturnValue.ALREADY_EXISTS;
            return ReturnValue.ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

    }

    public static Test getTestProfile(Integer testID, Integer semester) {
        /*
            Input: Test ID and semester.
            Output: The test profile (a test object) in case the test exists. BadTest() otherwise.
        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT *\n"
                    +"FROM test WHERE test_id = ? AND semester = ?");
            pstmt.setInt(1, testID);
            pstmt.setInt(2, semester);
            ResultSet results = pstmt.executeQuery();
            if (!results.next()) {
                results.close();
                return Test.badTest();
            } else {
                Test test = new Test();
                test.setId(results.getInt(1));
                test.setSemester(results.getInt(2));
                test.setTime(results.getInt(3));
                test.setRoom(results.getInt(4));
                test.setDay(results.getInt(5));
                test.setCreditPoints(results.getInt(6));

                return test;
            }

        } catch (SQLException e) {
            return Test.badTest();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ReturnValue deleteTest(Integer testID, Integer semester) {
        /*
          Input: Test ID and semester to be deleted.
          Output: ReturnValue with the following conditions:
         */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM test WHERE test_id = ? AND semester = ?");
            pstmt.setInt(1, testID);
            pstmt.setInt(2, semester);
            int results = pstmt.executeUpdate();
            if (results == 0) {
                return ReturnValue.NOT_EXISTS;
            }
            return ReturnValue.OK;
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ReturnValue addStudent(Student student) {
        /*
          Input: Student to be added.
          Output: ReturnValue with the following conditions:
         */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO student" +
                    " VALUES (?, ?, ?, ?)");
            pstmt.setInt(1,student.getId());
            pstmt.setString(2, student.getName());
            pstmt.setString(3,student.getFaculty());
            pstmt.setInt(4,student.getCreditPoints());

            pstmt.execute();
            return OK;

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ReturnValue.ALREADY_EXISTS;
            return ReturnValue.ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static Student getStudentProfile(Integer studentID) {
        /*
           Input: Student id.
           Output: The student with studentID if exists. BadStudent() otherwise.
        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT *\n"
                    +"FROM student WHERE student_id = ? ");
            pstmt.setInt(1, studentID);
            ResultSet results = pstmt.executeQuery();
            if (!results.next()) {
                results.close();
                return Student.badStudent();
            } else {
                Student student = new Student();
                student.setId(results.getInt(1));
                student.setName(results.getString(2));
                student.setFaculty(results.getString(3));
                student.setCreditPoints(results.getInt(4));

                return student;
            }

        } catch (SQLException e) {
            return Student.badStudent();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ReturnValue deleteStudent(Integer studentID) {
        /*
            Input: Student ID to be deleted.
            Output: ReturnValue with the following conditions:
        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM student WHERE student_id = ?");
            pstmt.setInt(1, studentID);
            int results = pstmt.executeUpdate();
            if (results == 0) {
                return ReturnValue.NOT_EXISTS;
            }
            return ReturnValue.OK;
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ReturnValue addSupervisor(Supervisor supervisor) {
        /*
            Input: Supervisor to be added.
            Output: ReturnValue with the following conditions:
        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO supervisor" +
                    " VALUES (?, ?, ?)");
            pstmt.setInt(1,supervisor.getId());
            pstmt.setString(2, supervisor.getName());
            pstmt.setInt(3,supervisor.getSalary());
            pstmt.execute();
            return OK;

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ReturnValue.ALREADY_EXISTS;
            return ReturnValue.ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static Supervisor getSupervisorProfile(Integer supervisorID) {
        /*
            Input: Supervisor id.
            Output: The supervisor with SupervisorID if exists. BadSupervisor() otherwise.
        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT *\n"
                    +"FROM supervisor WHERE supervisor_id = ? ");
            pstmt.setInt(1, supervisorID);
            ResultSet results = pstmt.executeQuery();
            if (!results.next()) {
                results.close();
                return Supervisor.badSupervisor();
            } else {
                Supervisor supervisor = new Supervisor();
                supervisor.setId(results.getInt(1));
                supervisor.setName(results.getString(2));
                supervisor.setSalary(results.getInt(3));

                return supervisor;
            }

        } catch (SQLException e) {
            return Supervisor.badSupervisor();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ReturnValue deleteSupervisor(Integer supervisorID) {
        /*
            Input: Supervisor ID to be deleted.
            Output: ReturnValue with the following conditions:
        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM supervisor WHERE supervisor_id = ?");
            pstmt.setInt(1, supervisorID);
            int results = pstmt.executeUpdate();
            if (results == 0) {
                return ReturnValue.NOT_EXISTS;
            }
            return ReturnValue.OK;
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ReturnValue studentAttendTest(Integer studentID, Integer testID, Integer semester) {
        /*
        Output: ReturnValue with the following conditions:
            * OK in case of success.
            * NOT_EXISTS if student/test does not exist.
            * ALREADY_EXISTS if the student already attending the test.
            * ERROR in case of a database error


        INSERT INTO attendances (studentID, testID, semester)
        ((SELECT id from students WHERE id=${studentID}),
        (SELECT id, semester from tests WHERE id=${testID} AND semester=${semester})

        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO attendance VALUES (?,?,?)");
            pstmt.setInt(1, studentID);
            pstmt.setInt(2, testID);
            pstmt.setInt(3, semester     );
            pstmt.execute();
            return ReturnValue.OK;

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue())
                return ReturnValue.NOT_EXISTS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ReturnValue.ALREADY_EXISTS;
            return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ReturnValue studentWaiveTest(Integer studentID, Integer testID, Integer semester) {
        /*
        Output: ReturnValue with the following conditions:
        * OK in case of success
        * NOT_EXISTS if student/test does not exist or student does not attend it.
        * ERROR in case of a database error

        DELETE FROM attendances
        WHERE studentID=(SELECT id from students WHERE id=${studentID})
        AND (testID,semester)=(SELECT id, semester from tests WHERE id=${testID} AND semester=${semester})

        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM attendance where student_id = ?"+
                    " AND test_id = ? AND semester = ?");
            pstmt.setInt(1, studentID);
            pstmt.setInt(2, testID);
            pstmt.setInt(3, semester);
            int results = pstmt.executeUpdate();
            if (results == 0) {
                return ReturnValue.NOT_EXISTS;
            }
            return ReturnValue.OK;

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue())
                return ReturnValue.NOT_EXISTS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ReturnValue.ALREADY_EXISTS;
            return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ReturnValue supervisorOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        /*

        INSERT INTO overseeing (supervisorID, testID, semester)
        ((SELECT id from supervisor WHERE id=${supervisorID}),
        (SELECT id, semester from tests WHERE id=${testID} AND semester=${semester})

        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO overseeing VALUES (?,?,?)");
            pstmt.setInt(1, supervisorID);
            pstmt.setInt(2, testID);
            pstmt.setInt(3, semester);
            pstmt.execute();
            return ReturnValue.OK;

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue())
                return ReturnValue.NOT_EXISTS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ReturnValue.ALREADY_EXISTS;
            return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ReturnValue supervisorStopsOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM overseeing where supervisor_id = ?"+
                    " AND test_id = ? AND semester = ?");
            pstmt.setInt(1, supervisorID);
            pstmt.setInt(2, testID);
            pstmt.setInt(3, semester);
            int results = pstmt.executeUpdate();
            if (results == 0) {
                return ReturnValue.NOT_EXISTS;
            }
            return ReturnValue.OK;

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return ReturnValue.BAD_PARAMS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue())
                return ReturnValue.NOT_EXISTS;
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ReturnValue.ALREADY_EXISTS;
            return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static Float averageTestCost() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT AVG(avg_salary) FROM test_avg_costs");

            ResultSet res = pstmt.executeQuery();
            if (!res.next()) {
                res.close();
                return 0.0f;
            } else {
                float money = res.getFloat(1);
                res.close();
                return money;
            }
        } catch (SQLException e) {
            return 0.0f;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static Integer getWage(Integer supervisorID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT salary FROM supervisor_salaries "+
                    "WHERE supervisor_id = ?");
            pstmt.setInt(1,supervisorID);
            ResultSet res = pstmt.executeQuery();
            if (!res.next()) {
                res.close();
                return -1;
            } else {
                int money = res.getInt(1);
                res.close();
                return money;
            }
        } catch (SQLException e) {
            return 0;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ArrayList<Integer> supervisorOverseeStudent() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> returnList = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement(
                    "SELECT student_id FROM (\n" +
                            "SELECT DISTINCT student_id, COUNT(*) AS count\n" +
                            "\tFROM attendance AS A, overseeing AS O\n" +
                            "\tWHERE A.test_id = O.test_id \n" +
                            "\tAND A.semester = O.semester\n" +
                            "\tGROUP BY student_id,supervisor_id\n" +
                            "\t) AS T\n" +
                            "\tWHERE T.count > 1 ORDER BY student_id DESC");
            ResultSet res = pstmt.executeQuery();
            while (res.next()) {
                returnList.add(res.getInt(1));
            }
            res.close();
            return returnList;
        } catch (SQLException e) {
            return returnList;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ArrayList<Integer> testsThisSemester(Integer semester) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> returnList = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement(
                    "SELECT test_id \n" +
                            "FROM test \n" +
                            "WHERE semester = ?\n" +
                            "ORDER BY test_id DESC\n" +
                            "LIMIT 5");
            pstmt.setInt(1,semester);
            ResultSet res = pstmt.executeQuery();
            while (res.next()) {
                returnList.add(res.getInt(1));
            }
            res.close();
            return returnList;
        } catch (SQLException e) {
            return returnList;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static Boolean studentHalfWayThere(Integer studentID) {
        /*
            SELECT EXISTS
            (SELECT * FROM student WHERE studentID=${studentID} AND CreditPoints >=
                (SELECT points/2 FROM credit_points WHERE faculty = student.faculty)
            )
        */
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM student AS S\n" +
                    "LEFT JOIN creditpoints AS C\n" +
                    "ON S.faculty = C.faculty\n" +
                    "WHERE S.credit_points >= C.points/2 AND\n" +
                    "\tstudent_id = ?");

            pstmt.setInt(1, studentID);
            ResultSet res = pstmt.executeQuery();
            if (!res.next()) {
                res.close();
                return Boolean.FALSE;

            } else {
                res.close();
                return Boolean.TRUE;
            }
        } catch (SQLException e) {
            return Boolean.FALSE;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static Integer studentCreditPoints(Integer studentID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT points\n" +
                    "FROM student_after_tests_points\n" +
                    "WHERE student_id = ?");
            pstmt.setInt(1,studentID);
            ResultSet res = pstmt.executeQuery();
            if (!res.next()) {
                res.close();
                return 0;
            } else {
                int points = res.getInt(1);
                res.close();
                return points;
            }
        } catch (SQLException e) {
            return 0;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static Integer getMostPopularTest(String faculty) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT test_id FROM (\n" +
                    "SELECT test_id, count(S.student_id) AS students\n" +
                    "FROM student AS S \n" +
                    "LEFT JOIN attendance AS T\n" +
                    "ON S.student_id = T.student_id\n" +
                    "WHERE faculty = ?\n" +
                    "GROUP BY test_id\n" +
                    "ORDER BY students DESC, test_id DESC\n" +
                    "LIMIT 1\n" +
                    ") AS TAB");
            pstmt.setString(1,faculty);
            ResultSet res = pstmt.executeQuery();
            if (!res.next()) {
                res.close();
                return 0;
            } else {
                int popularTest = res.getInt(1);
                res.close();
                return popularTest;
            }
        } catch (SQLException e) {
            return 0;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ArrayList<Integer> getConflictingTests() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> returnList = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement(
                    "SELECT A.test_id\n" +
                            "FROM test AS A, test AS B\n" +
                            "WHERE A.test_id <> B.test_id\n" +
                            "AND A.semester = B.semester\n" +
                            "AND A.time = B.time\n" +
                            "AND A.day = B.day\n" +
                            "ORDER BY A.test_id ASC");
            ResultSet res = pstmt.executeQuery();
            while (res.next()) {
                returnList.add(res.getInt(1));
            }
            res.close();
            return returnList;
        } catch (SQLException e) {
            return returnList;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ArrayList<Integer> graduateStudents() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> returnList = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement(
                    "SELECT SA.student_id\n" +
                            "FROM student_after_tests_points AS SA,\n" +
                            "student AS S, creditpoints AS C\n" +
                            "WHERE SA.student_id = S.student_id\n" +
                            "AND C.faculty = S.faculty \n" +
                            "AND SA.points >= C.points\n" +
                            "ORDER BY SA.student_id ASC\n" +
                            "LIMIT 5");
            ResultSet res = pstmt.executeQuery();
            while (res.next()) {
                returnList.add(res.getInt(1));
            }
            res.close();
            return returnList;
        } catch (SQLException e) {
            return returnList;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static ArrayList<Integer> getCloseStudents(Integer studentID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> returnList = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("SELECT T.id\n" +
                    "FROM (\n" +
                    "\tSELECT A.student_id , B.student_id AS id, count(*) as count\n" +
                    "\tfrom attendance AS A, attendance AS B\n" +
                    "\tWHERE A.student_id = ?\n" +
                    "\tAND A.test_id = B.test_id\n" +
                    "\tAND A.semester = B.semester\n" +
                    "\tAND B.student_id <> ?\n" +
                    "\tGROUP BY A.student_id, B.student_id\n" +
                    ") AS T\n" +
                    "WHERE T.count >= (\n" +
                    "\tSELECT count(*)\n" +
                    "\tFROM attendance\n" +
                    "\tWHERE student_id = ?\n" +
                    ")/2.0\n" +
                    "ORDER BY id DESC\n" +
                    "LIMIT 10\n"+
                    "\n" +
                    "UNION\n" +
                    "SELECT student_id AS id\n" +
                    "FROM student\n" +
                    "WHERE student_id <> ?\n" +
                    "AND NOT EXISTS(\n" +
                    "\tSELECT *\n" +
                    "\tFROM attendance\n" +
                    "\tWHERE student_id = ?\n" +
                    ")\n" +
                    "ORDER BY id DESC\n" +
                    "LIMIT 10");
            pstmt.setInt(1,studentID);
            pstmt.setInt(2,studentID);
            pstmt.setInt(3,studentID);
            pstmt.setInt(4,studentID);
            pstmt.setInt(5,studentID);
            ResultSet res = pstmt.executeQuery();
            while (res.next()) {
                returnList.add(res.getInt(1));
            }
            res.close();
            return returnList;
        } catch (SQLException e) {
            return returnList;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }
    public static void main(String[] args) {
        /*
        InitialState.createInitialState();
        createTables();
        Test test = new Test();
        test.setId(1);
        test.setSemester(1);
        test.setTime(1);
        test.setDay(1);
        test.setRoom(233);
        test.setCreditPoints(3);
        ReturnValue ret = addTest(test);
        assertEquals(ReturnValue.OK, ret);

        DBConnector.printTablesSchemas();
        dropTables();
        */
    }

}
