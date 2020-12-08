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


public class Solution {
    public static void createTables()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("CREATE TABLE Supervisor\n" +
                    "(\n" +
                    "    id integer NOT NULL,\n" +
                    "    name text NOT NULL ,\n" +
                    "    salary integer NOT NULL,\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0 AND salary >= 0) \n" +
                    ")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE VIEW Attendance\n" +
                    "(\n" +
                    "SELECT * FROM \n" +
                    "Overseeing AS O INNER JOIN\n" +
                    "Supervisor AS S\n"+
                    "ON O.supervisorID=S.supervisorID)");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE VIEW supervisors_overseeing\n" +
                    "(\n" +
                            "SELECT * FROM \n" +
                            "Overseeing AS O INNER JOIN\n" +
                            "Supervisor AS S\n"+
                            "ON O.supervisorID=S.supervisorID)");
            pstmt.execute();

        } catch (SQLException e) {
            //e.printStackTrace()();
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
        //clear your tables here
    }

    public static void dropTables() {
        InitialState.dropInitialState();
		//drop your tables here
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
       return OK;
    }

    public static Test getTestProfile(Integer testID, Integer semester) {
        /*
            Input: Test ID and semester.
            Output: The test profile (a test object) in case the test exists. BadTest() otherwise.
        */
        return new Test();
    }

    public static ReturnValue deleteTest(Integer testID, Integer semester) {
        /*
          Input: Test ID and semester to be deleted.
          Output: ReturnValue with the following conditions:
         */
		return OK;
    }

    public static ReturnValue addStudent(Student student) {
        /*
          Input: Student to be added.
          Output: ReturnValue with the following conditions:
         */
        return OK;
    }

    public static Student getStudentProfile(Integer studentID) {
        /*
           Input: Student id.
           Output: The student with studentID if exists. BadStudent() otherwise.
        */
        return new Student();
    }

    public static ReturnValue deleteStudent(Integer studentID) {
        /*
            Input: Student ID to be deleted.
            Output: ReturnValue with the following conditions:
        */
        return OK;
    }

    public static ReturnValue addSupervisor(Supervisor supervisor) {
        /*
            Input: Supervisor to be added.
            Output: ReturnValue with the following conditions:
        */
        return OK;
    }

    public static Supervisor getSupervisorProfile(Integer supervisorID) {
        /*
            Input: Supervisor id.
            Output: The supervisor with SupervisorID if exists. BadSupervisor() otherwise.
        */
        return new Supervisor();
    }

    public static ReturnValue deleteSupervisor(Integer supervisorID) {
        /*
            Input: Supervisor ID to be deleted.
            Output: ReturnValue with the following conditions:
        */
        return OK;
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
        return OK;
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
        return OK;
    }

    public static ReturnValue supervisorOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        /*

        INSERT INTO overseeing (supervisorID, testID, semester)
        ((SELECT id from supervisor WHERE id=${supervisorID}),
        (SELECT id, semester from tests WHERE id=${testID} AND semester=${semester})

        */
       return OK;
    }

    public static ReturnValue supervisorStopsOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        /*
        DELETE FROM overseeing
        WHERE supervisorID=(SELECT id from supervisor WHERE id=${supervisorID})
        AND (testID,semester)=(SELECT id, semester from tests WHERE id=${testID} AND semester=${semester})

        */
       return OK;
    }

    public static Float averageTestCost() {
        /*
            SELECT AVG(average_test) FROM (
                SELECT AVG(salary) AS average_test
                FROM (SELECT * FROM Overseeing AS O INNER JOIN
                 Supervisor AS S ON O.supervisorID=S.supervisorID)
                GROUP BY testID, semester
            )
        */
        return 0.0f;
    }

    public static Integer getWage(Integer supervisorID) {
        /*
        TODO: add views: attendance, overseeing, regular tables
        SELECT SUM(salary) FROM supervisors_overseeing WHERE supervisorID=${supervisorID}
        output:
            -1 if error
        */
        return 0;
    }

    public static ArrayList<Integer> supervisorOverseeStudent() {
        /*
            SELECT studentID from (
                SELECT studentID,supervisorID
                FROM Attendance
                INNER JOIN Overseeing
                ON (Attendance.testID, Attendance.semester)=
                 (Overseeing.testID,Overseeing.semester)
                 GROUP BY studentID, supervisorID
                 HAVING count(*) > 1
               )
               ORDER BY studentID DESC
        */
        return new ArrayList<Integer>();
    }

    public static ArrayList<Integer> testsThisSemester(Integer semester) {
        /*
            SELECT TOP 5 testID FROM test
            WHERE Semester=${semester}
            ORDER BY testID DESC

        */
        return new ArrayList<Integer>();
    }

    public static Boolean studentHalfWayThere(Integer studentID) {
        /*
            SELECT EXISTS
            (SELECT * FROM student WHERE studentID=${studentID} AND CreditPoints >=
                (SELECT points/2 FROM credit_points WHERE faculty = student.faculty)
            )
        */
        return true;
    }

    public static Integer studentCreditPoints(Integer studentID) {
        /*
            SELECT creditPoints
            +(SELECT SUM(TestCreditPoints) FROM attendance WHERE studentID=${studentID})
             FROM student WHERE studentID=${studentID}
        */
        return 0;
    }

    public static Integer getMostPopularTest(String faculty) {
        /*
            SELECT TOP 1 testID FROM (SELECT testID, COUNT(*) AS testCount
            FROM attendance
            WHERE faculty=${faculty}
            GROUP BY testID
            ORDER BY testCount, testID DESC
            )
        */
        return 0;
    }

    public static ArrayList<Integer> getConflictingTests() {
        /*
            SELECT t1.testID
            FROM test AS t1, test AS t2
            WHERE (t1.testID <> t2.testID OR t1.semester<>t2.semester)
            AND (t1.semester=t2.semester AND t1.day=t2.day AND t1.time=t2.time)
            ORDER BY testID ASC
        */
        return new ArrayList<Integer>();
    }

    public static ArrayList<Integer> graduateStudents() {
        return new ArrayList<Integer>();
    }

    public static ArrayList<Integer> getCloseStudents(Integer studentID) {
        return new ArrayList<Integer>();
    }
    public static void main(String[] args) {
        createTables();
        DBConnector.printTablesSchemas();
    }

}
