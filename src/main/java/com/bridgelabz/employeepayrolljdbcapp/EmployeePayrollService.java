package com.bridgelabz.employeepayrolljdbcapp;

import java.util.List;
import java.util.Scanner;

import com.bridgelabz.employeepayrolljdbcapp.EmployeePayrollException.ExceptionType;

import java.time.LocalDate;
import java.util.ArrayList;

public class EmployeePayrollService {
	
	private List<EmployeePayrollData> employeePayrollList;
	private EmployeePayrollDBService employeePayrollDBService;

	public enum IOService {
		CONSOLE_IO, FILE_IO, DB_IO, REST_IO
	}
	
	public EmployeePayrollService() {
		employeePayrollDBService =  EmployeePayrollDBService.getInstance();
	}
	
	public EmployeePayrollService(List<EmployeePayrollData> employeePayrollList) {
		this();
		this.employeePayrollList = employeePayrollList;
	}
	
	public void printData(IOService fileIo) {
		if(fileIo.equals(IOService.FILE_IO)) new EmployeePayrollFileIOService().printData();
	}

	public long countEntries(IOService fileIo) {
		if(fileIo.equals(IOService.FILE_IO)) 
			return new EmployeePayrollFileIOService().countEntries();
		return 0;
	}
	
	private void readEmployeePayrollData(Scanner consoleInputReader) {
		
		System.out.println("Enter the Employee Id : ");
		int id = consoleInputReader.nextInt();
		System.out.println("Enter the Employee Name : ");
		String name = consoleInputReader.next();
		System.out.println("Enter the Employee Salary : ");
		double salary = consoleInputReader.nextDouble();
		
		employeePayrollList.add(new EmployeePayrollData(id, name, salary));
	}
	
	private EmployeePayrollData getEmployeePayrollData(String name) {
		
		return this.employeePayrollList.stream()
				.filter(EmployeePayrollDataItem -> EmployeePayrollDataItem.employeeName.equals(name))
				.findFirst()
				.orElse(null);
	}
	
	public long readDataFromFile(IOService fileIo) {
		
		List<String> employeePayrollFromFile = new ArrayList<String>();
		if(fileIo.equals(IOService.FILE_IO)) {
			System.out.println("Employee Details from payroll-file.txt");
			employeePayrollFromFile = new EmployeePayrollFileIOService().readDataFromFile();
			
		}
		return employeePayrollFromFile.size();
	}
	
	public void writeEmployeePayrollData(IOService ioService) {
		
		if(ioService.equals(IOService.CONSOLE_IO))
			System.out.println("\nWriting Employee Payroll Roster to Console\n" + employeePayrollList);
		
		else if(ioService.equals(IOService.FILE_IO))
			new EmployeePayrollFileIOService().writeData(employeePayrollList);
	}	
	
	public List<EmployeePayrollData> readEmployeePayrollData(IOService ioService) {
		
		try {
			if (ioService.equals(IOService.DB_IO))
				this.employeePayrollList = employeePayrollDBService.readData();
			return this.employeePayrollList;
		} 
		catch (EmployeePayrollException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		
	}
	
	public void updateEmployeeSalary(String name, double salary) {
		
		int result = employeePayrollDBService.updateEmployeeData(name,salary);
		if(result == 0) 
			return;
		
		EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
		if(employeePayrollData != null)
			employeePayrollData.payrollDetails.basicSalary = salary;
		
	}	

	public void updateEmployeeSalaryUsingStatement(String name, double salary) {
		
		int result = employeePayrollDBService.updateEmployeeDataUsingStatement(name,salary);
		if(result == 0) 
			return;
		
		EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
		if(employeePayrollData != null)
			employeePayrollData.payrollDetails.basicSalary = salary;		
	}
	
	public boolean checkEmployeePayrollInSyncWithDB(String name) {
		
		List<EmployeePayrollData> employeePayrollDataList = employeePayrollDBService.getEmployeePayrollData(name);
		return employeePayrollDataList.get(0).equals(getEmployeePayrollData(name));
	}
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnName(IOService ioService, String name) {
		if(ioService.equals(IOService.DB_IO))
			this.employeePayrollList = employeePayrollDBService.getEmployeeDetailsBasedOnNameUsingStatement(name);
		return this.employeePayrollList;
	}
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnStartDate(IOService ioService, LocalDate startDate, LocalDate endDate) {
		if(ioService.equals(IOService.DB_IO))
			this.employeePayrollList = employeePayrollDBService.getEmployeeDetailsBasedOnStartDateUsingStatement(startDate, endDate);
		return this.employeePayrollList;
	}
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnStartDateUsingPreparedStatement(IOService ioService, String startDate, String endDate) {
		
		if(ioService.equals(IOService.DB_IO))
			this.employeePayrollList = employeePayrollDBService.getEmployeeDetailsBasedOnStartDateUsingPreparedStatement(startDate, endDate);
		return this.employeePayrollList;
	}
	
	public List<Double> getSumOfSalaryBasedOnGender(IOService ioService) {
		
		List<Double> sumOfSalaryBasedOnGender = new ArrayList<Double>();
		if(ioService.equals(IOService.DB_IO))
			sumOfSalaryBasedOnGender = employeePayrollDBService.getSumOfSalaryBasedOnGenderUsingStatement();
		return sumOfSalaryBasedOnGender;	
	}

	public List<Double> getAverageOfSalaryBasedOnGender(IOService ioService) {
		
		List<Double> averageOfSalaryBasedOnGender = new ArrayList<Double>();
		if(ioService.equals(IOService.DB_IO))
			averageOfSalaryBasedOnGender = employeePayrollDBService.getAverageOfSalaryBasedOnGenderUsingStatement();
		return averageOfSalaryBasedOnGender;
	}

	public List<Double> getMinimumSalaryBasedOnGender(IOService ioService) {
		
		List<Double> minimumSalaryBasedOnGender = new ArrayList<Double>();
		if(ioService.equals(IOService.DB_IO))
			minimumSalaryBasedOnGender = employeePayrollDBService.getMinimumSalaryBasedOnGenderUsingStatement();
		return minimumSalaryBasedOnGender;
	}
	
	public List<Double> getMaximumSalaryBasedOnGender(IOService ioService) {
		
		List<Double> maximumSalaryBasedOnGender = new ArrayList<Double>();
		if(ioService.equals(IOService.DB_IO))
			maximumSalaryBasedOnGender = employeePayrollDBService.getMaximumSalaryBasedOnGenderUsingStatement();
		return maximumSalaryBasedOnGender;
	}

	public List<Integer> getCountOfEmployeesBasedOnGender(IOService ioService) {
	
		List<Integer> countBasedOnGender = new ArrayList<Integer>();
		if(ioService.equals(IOService.DB_IO))
			countBasedOnGender = employeePayrollDBService.getCountOfEmployeesBasedOnGenderUsingStatement();
		return countBasedOnGender;
	}
	
	public void addEmployeeToPayroll(String name, double salary, LocalDate start, String gender) {

		employeePayrollList.add(employeePayrollDBService.addEmployeeToPayroll(name, salary, start, gender));
	}
	
	public void addEmployeeToUpdatedDatabse(String name, double salary, LocalDate start, String gender) {

		employeePayrollList.add(employeePayrollDBService.addEmployeeToUpdatedDatabase(name, salary, start, gender));
	}

	public static void main(String[] args) {
		
		System.out.println("---------- Welcome To Employee Payroll Application ----------\n");
		ArrayList<EmployeePayrollData> employeePayrollList  = new ArrayList<EmployeePayrollData>();
		EmployeePayrollService employeePayrollService = new EmployeePayrollService(employeePayrollList);
		Scanner consoleInputReader = new Scanner(System.in);
		
		employeePayrollService.readEmployeePayrollData(consoleInputReader);
		employeePayrollService.writeEmployeePayrollData(IOService.CONSOLE_IO);		
	}

	
}
