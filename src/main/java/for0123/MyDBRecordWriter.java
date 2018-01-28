package for0123;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyDBRecordWriter extends RecordWriter<Flow,NullWritable> {
    private static Connection connection;
    private static PreparedStatement preparedStatement;
    static {
        try {
            connection= DriverManager.getConnection("jdbc:mysql://localhost:3306/for1023","root","root");
            preparedStatement=connection.prepareStatement("INSERT INTO phone1 (phonenumber, up, down, allv) VALUES (?,?,?,?)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void write(Flow flow, NullWritable nullWritable) throws IOException, InterruptedException {
        try {
            preparedStatement.setString(1,flow.getPhoneNumber());
            preparedStatement.setInt(2,flow.getUp());
            preparedStatement.setInt(3,flow.getDown());
            preparedStatement.setInt(4,flow.getAll());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            preparedStatement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
