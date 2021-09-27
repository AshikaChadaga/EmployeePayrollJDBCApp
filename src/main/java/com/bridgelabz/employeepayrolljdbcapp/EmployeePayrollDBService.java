package com.bridgelabz.employeepayrolljdbcapp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class EmployeePayrollDBService {
	
	private PreparedStatement employeePayrollDataStatement;
	private static EmployeePayrollDBService employeePayrollDBService;
	
	private EmployeePayrollDBService() {
		
	}
	
	public static EmployeePayrollDBService getInstance() {
		if(employeePayrollDBService == null)
			employeePayrollDBService = new EmployeePayrollDBService();
		return employeePayrollDBService;
	}

	private Connection getConnection() throws SQLException {
		
		String jdbcURL = "jdbc:mysql://localhost:3306/employeepayroll_service?useSSL=false";
		String userName = "root";
		String password = "Bridgelabz@1234";
		Connection connection;
		
		System.out.println("Connecting to the database : "+jdbcURL);
		connection = DriverManager.getConnection(jdbcURL, userName, password);
		System.out.println("Connection is Succcessfully Established!! "+connection);
		
		return connection;
	}
	
	private List<EmployeePayrollData> getEmployeePayrollData(ResultSet resultSet) {
		
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
		
		try {
			while(resultSet.next()) {
				int id = resultSet.getInt("id");
				String name = resultSet.getString("name");
				double basicSalary = resultSet.getDouble("salary");
				LocalDate startDate = resultSet.getDate("start").toLocalDate();
				employeePayrollList.add(new EmployeePayrollData(id, name, basicSalary, startDate));
			}
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
		
	}
	
	public List<EmployeePayrollData> getEmployeePayrollData(String name) {
		
		List<EmployeePayrollData> employeePayrollList = null;
		if(this.employeePayrollDataStatement == null)
			this.prepareStatementForEmployeeData();
		try {
			employeePayrollDataStatement.setString(1,name);
			ResultSet resultSet = employeePayrollDataStatement.executeQuery();
			employeePayrollList = this.getEmployeePayrollData(resultSet);	
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
	}

	
	public List<EmployeePayrollData> readData(){
		
		String sqlStatement = "SELECT * FROM employee_payroll;";
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return employeePayrollList;
	}
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnNameUsingStatement(String name) {
		
		String sqlStatement = String.format("SELECT * FROM employee_payroll WHERE name = '%s';",name);
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return employeePayrollList;
		
	}
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnStartDateUsingStatement(String startDate) {
		
		String sqlStatement = String.format("SELECT * FROM employee_payroll WHERE start BETWEEN CAST('%s' AS DATE) AND DATE(NOW());",startDate);
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return employeePayrollList;
	}
	
	public List<Double> getSumOfSalaryBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, SUM(salary) AS TotalSalary FROM employee_payroll GROUP BY gender;";
		List<Double> sumOfSalaryBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				double salary = resultSet.getDouble("TotalSalary");
				sumOfSalaryBasedOnGender.add(salary);
			}
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return sumOfSalaryBasedOnGender;
	}
	
	public List<Double> getAverageOfSalaryBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, AVG(salary) AS AverageSalary FROM employee_payroll GROUP BY gender;";
		List<Double> averageOfSalaryBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				double salary = resultSet.getDouble("AverageSalary");
				averageOfSalaryBasedOnGender.add(salary);
			}
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return averageOfSalaryBasedOnGender;
	}
	
	public List<Double> getMinimumSalaryBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, MIN(salary) AS MinimumSalary FROM employee_payroll GROUP BY gender;";
		List<Double> MinimumSalaryBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				double salary = resultSet.getDouble("MinimumSalary");
				MinimumSalaryBasedOnGender.add(salary);
			}
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return MinimumSalaryBasedOnGender;
	}
	
	public List<Double> getMaximumSalaryBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, MAX(salary) AS MaximumSalary FROM employee_payroll GROUP BY gender;";
		List<Double> MaximumSalaryBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				double salary = resultSet.getDouble("MaximumSalary");
				MaximumSalaryBasedOnGender.add(salary);
			}
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return MaximumSalaryBasedOnGender;
	}
	
	public List<Integer> getCountOfEmployeesBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, COUNT(gender) AS CountBasedOnGender FROM employee_payroll GROUP BY gender;";
		List<Integer> CountBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				int count = resultSet.getInt("CountBasedOnGender");
				CountBasedOnGender.add(count);
			}
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return CountBasedOnGender;
	}

	private void prepareStatementForEmployeeData() {
		
		try {
			Connection connection = this.getConnection();
			String sqlStatement = "SELECT * FROM employee_payroll WHERE name = ?;";
			employeePayrollDataStatement = connection.prepareStatement(sqlStatement);
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}

	public int updateEmployeeData(String name, double salary) {
		
		return this.updateEmployeeDataUsingStatement(name,salary);
	}
	
	

	public int updateEmployeeDataUsingStatement(String name, double salary) {
		
		String sqlStatement = String.format("UPDATE employee_payroll SET salary = %.2f WHERE name = '%s';", salary, name);
		
		try (Connection connection = getConnection()){
			Statement statement = connection.createStatement();
			return statement.executeUpdate(sqlStatement);
		}
		catch(SQLException e){
			e.printStackTrace();
		}		
		return 0;
	}
	
}
