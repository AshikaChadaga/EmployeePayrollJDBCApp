package com.bridgelabz.employeepayrolljdbcapp;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import com.bridgelabz.employeepayrolljdbcapp.EmployeePayrollException.ExceptionType;

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
		
		String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
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
				int id = resultSet.getInt("employee_id");
				String name = resultSet.getString("employee_name");
				double basicSalary = resultSet.getDouble("basic_salary");
				LocalDate startDate = resultSet.getDate("start_date").toLocalDate();
				employeePayrollList.add(new EmployeePayrollData(id, name, basicSalary, startDate));
			}
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
		
	}
	
	public List<EmployeePayrollData> getEmployeePayrollData(String name) {
		
		List<EmployeePayrollData> employeePayrollList = null;
		if(this.employeePayrollDataStatement == null)
			this.preparedStatementForEmployeeData();
		try {
			employeePayrollDataStatement.setString(1,name);
			ResultSet resultSet = employeePayrollDataStatement.executeQuery();
			employeePayrollList = this.getEmployeePayrollData(resultSet);	
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
	}
	
	public List<EmployeePayrollData> readData(){
		
		String sqlStatement = "SELECT * FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id;";
		return this.getEmployeePayrollDataUsingDB(sqlStatement);
	}
	
	private void preparedStatementForEmployeeData() {
		
		try {
			Connection connection = this.getConnection();
			String sqlStatement = "SELECT * FROM employee_payroll WHERE name = ?;";
			employeePayrollDataStatement = connection.prepareStatement(sqlStatement);
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
	}
	
	private void preparedStatementForEmployeeDataBasedOnStartDate() {
		
		try {
			Connection connection = this.getConnection();
			String sqlStatement = "SELECT * FROM employee_payroll WHERE start BETWEEN ? AND ?;";
			employeePayrollDataStatement = connection.prepareStatement(sqlStatement);
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
	}
	
	private List<EmployeePayrollData> getEmployeePayrollDataUsingDB (String sqlStatement) {
		
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
		
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
	}
	
	
	public int updateEmployeeData(String name, double salary) {
		
		return this.updateEmployeeDataUsingStatement(name,salary);
	}	

	private int updateEmployeeDataUsingStatement(String name, double salary) {
		
		String sqlStatement = String.format("UPDATE employee_payroll SET salary = %.2f WHERE name = '%s';", salary, name);
		
		try (Connection connection = getConnection()){
			Statement statement = connection.createStatement();
			return statement.executeUpdate(sqlStatement);
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(ExceptionType.UPDATE_FAILED, exception.getMessage());
		}
	}
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnNameUsingStatement(String name) {
		
		String sqlStatement = String.format("SELECT * FROM employee_payroll WHERE name = '%s';",name);
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
		
	}
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnStartDateUsingStatement(LocalDate startDate, LocalDate endDate) {
		
		String sqlStatement = String.format("SELECT * FROM employee_payroll WHERE start BETWEEN '%s' AND '%s';",startDate, endDate);
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
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
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
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
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
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
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());

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
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
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
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return CountBasedOnGender;
	}	
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnStartDateUsingPreparedStatement(String startDate, String endDate) {
		
		List<EmployeePayrollData> employeePayrollList = null;
		if(this.employeePayrollDataStatement == null)
			this.preparedStatementForEmployeeDataBasedOnStartDate();
		try {
			employeePayrollDataStatement.setString(1,startDate);
			employeePayrollDataStatement.setString(2,endDate);
			ResultSet resultSet = employeePayrollDataStatement.executeQuery();
			employeePayrollList = this.getEmployeePayrollData(resultSet);	
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
	}
	
	public EmployeePayrollData addEmployeeToPayroll(String name, double salary, LocalDate startDate, String gender) {
		
		int employeeId = -1;
		EmployeePayrollData employeePayrollData = null;
		String sql = String.format("INSERT INTO employee_payroll (name, gender, salary, start) VALUES ('%s', '%s', '%s', '%s')", name, gender, salary, Date.valueOf(startDate));
		
		try (Connection connection = this.getConnection()){
			
			Statement statement = connection.createStatement();
			int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
			if(rowAffected == 1) {
				ResultSet resultSet = statement.getGeneratedKeys();
				if(resultSet.next())
					employeeId = resultSet.getInt(1);
			}
			employeePayrollData = new EmployeePayrollData(employeeId, name, salary, startDate);
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		
		return employeePayrollData;
		
	}
	
	public EmployeePayrollData addEmployeeToUpdatedDatabase(String name, double salary, LocalDate startDate, String gender) {
		
		int employeeId = -1;
		Connection connection = null;
		EmployeePayrollData employeePayrollData = null;
		
		try {
			connection = this.getConnection();
			connection.setAutoCommit(false);
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		try (Statement statement = connection.createStatement()){
			
			String sql = String.format("INSERT INTO employee_payroll (name, gender, salary, start) VALUES ('%s', '%s', '%s', '%s')", name, gender, salary, Date.valueOf(startDate));
			int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
			if(rowAffected == 1) {
				ResultSet resultSet = statement.getGeneratedKeys();
				if(resultSet.next())
					employeeId = resultSet.getInt(1);
			}
			employeePayrollData = new EmployeePayrollData(employeeId, name, salary, startDate);
		}
		catch(SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
				return employeePayrollData;
			} catch (SQLException exception) {
				throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.CONNECTION_FAILED, exception.getMessage());
			}
		}
		
		try(Statement statement = connection.createStatement()){

			double deductions = salary * 0.2;
			double taxablePay = salary - deductions;
			double tax = taxablePay * 0.1;
			double netPay = salary - tax;
			String sqlQuery = String.format("INSERT INTO payroll_details(employee_id, basic_pay, deductions, taxable_pay, tax, net_pay) values ('%s', '%s', '%s', '%s', '%s', '%s')",employeeId, salary, deductions, taxablePay, tax, netPay);
			int rowAffected = statement.executeUpdate(sqlQuery);
			if (rowAffected == 1) {
				employeePayrollData = new EmployeePayrollData(employeeId, name, salary, startDate);
			}			
		}
		catch(SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
			} catch (SQLException exception) {
				throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.CONNECTION_FAILED, exception.getMessage());
			}
		}
		
		try {
			connection.commit();
		} catch (SQLException e) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.COMMIT_FAILED, e.getMessage());
		}
		finally {
			if(connection != null)
				try {
					connection.close();
				} 
			catch (SQLException e) {
				throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.RESOURCES_NOT_CLOSED_EXCEPTION, e.getMessage());
			}
		}
		return employeePayrollData;
	}
	
}
