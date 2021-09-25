package com.bridgelabz.employeepayrolljdbcapp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class EmployeePayrollDBService {
	
	private Connection getConnection() throws SQLException {
		
		String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
		String userName = "root";
		String password = "Bridgelabz@1234";
		Connection connection;
		
		System.out.println("Connecting to the database : "+jdbcURL);
		connection = DriverManager.getConnection(jdbcURL, userName, password);
		System.out.println("Connection is Succcessfully Established!! "+connection);
		
		return connection;
	}
	
	public List<EmployeePayrollData> readData(){
		
		String sqlStatement = "SELECT employee.employee_id, employee_name, basic_salary, start_date FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id;";
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
				
		try (Connection connection = getConnection();){
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return employeePayrollList;
	}
}
