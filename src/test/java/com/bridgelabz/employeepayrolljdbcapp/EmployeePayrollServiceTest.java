package com.bridgelabz.employeepayrolljdbcapp;

import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

import com.bridgelabz.employeepayrolljdbcapp.EmployeePayrollService.IOService;


public class EmployeePayrollServiceTest 
{
	@Test
	public void given3Employees_WhenWrittenToFile_ShouldMatchEmployeeEntries()
	{
		EmployeePayrollData[] arrayOfEmployees = {
				new EmployeePayrollData(1, "Jeff Bezos", 100000.0),
				new EmployeePayrollData(2, "Bill Gates", 200000.0),
				new EmployeePayrollData(3, "Mark Zuckerberg", 300000.0)
		};
		EmployeePayrollService employeePayrollService;
		employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmployees));
		employeePayrollService.writeEmployeePayrollData(IOService.FILE_IO);
		
		employeePayrollService.printData(IOService.FILE_IO);
		long entries = employeePayrollService.countEntries(IOService.FILE_IO);
		Assert.assertEquals(3, entries);
		
	}
	
	@Test
	public void givenFile_WhenRead_ShouldReturnNumberOfEntries() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		long entries = employeePayrollService.readDataFromFile(IOService.FILE_IO);
		Assert.assertEquals(3, entries);
	}
	
	@Test
	public void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount(){
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		Assert.assertEquals(10, employeePayrollData.size());
	}
	
	@Test 
	public void givenNewSalaryForEmployee_WhenUpdatedUsingStatement_ShouldSyncWithDB() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		employeePayrollService.updateEmployeeSalaryUsingStatement("Rosa Diaz", 7000000.00);
		
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Rosa Diaz");
		Assert.assertTrue(result);
		
	}
	
	@Test 
	public void givenNewSalaryForEmployee_WhenUpdated_ShouldSyncWithDB() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		employeePayrollService.updateEmployeeSalary("Rosa Diaz", 10000000.00);
		
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Rosa Diaz");
		Assert.assertTrue(result);
		
	}
	
	@Test
	public void givenName_WhenFound_ShouldReturnEmployeeDetails() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		String name = "Rosa Diaz";
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.getEmployeeDetailsBasedOnName(IOService.DB_IO, name);
		String resultName = employeePayrollData.get(0).employeeName;
		Assert.assertEquals(name, resultName);
	}
	
	@Test
	public void givenStartDateRange_WhenMatches_ShouldReturnEmployeeDetails() {
		
		
		LocalDate startDate = LocalDate.of(2013, 01, 01);
		LocalDate endDate = LocalDate.of(2021, 01, 01);
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.getEmployeeDetailsBasedOnStartDate(IOService.DB_IO, startDate, endDate);
		Assert.assertEquals(6, employeePayrollData.size());
	}
	
	@Test
	public void givenEmployeePayrollInDB_ShouldReturnSumOfSalaryBasedOnGender() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<Double> expectedSalarySum = new ArrayList();
		expectedSalarySum.add(26000000.00);
		expectedSalarySum.add(30000000.00);
		List<Double> sumOfSalaryBasedOnGender = employeePayrollService.getSumOfSalaryBasedOnGender(IOService.DB_IO);
		if(sumOfSalaryBasedOnGender.size() == 2) {
			Assert.assertEquals(expectedSalarySum, sumOfSalaryBasedOnGender);
		}
		
	}
	
	@Test
	public void givenEmployeePayrollInDB_ShouldReturnAverageOfSalaryBasedOnGender() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<Double> expectedSalaryAverage = new ArrayList();
		expectedSalaryAverage.add(6500000.00);
		expectedSalaryAverage.add(5000000.00);
		List<Double> averageOfSalaryBasedOnGender = employeePayrollService.getAverageOfSalaryBasedOnGender(IOService.DB_IO);
		if(averageOfSalaryBasedOnGender.size() == 2) {
			Assert.assertEquals(expectedSalaryAverage, averageOfSalaryBasedOnGender);
		}
	}
	
	@Test
	public void givenEmployeePayrollInDB_ShouldReturnMinimumSalaryBasedOnGender() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<Double> expectedMinimumSalary = new ArrayList();
		expectedMinimumSalary.add(4000000.00);
		expectedMinimumSalary.add(1000000.00);
		List<Double> minimumSalaryBasedOnGender = employeePayrollService.getMinimumSalaryBasedOnGender(IOService.DB_IO);
		if(minimumSalaryBasedOnGender.size() == 2) {
			Assert.assertEquals(expectedMinimumSalary, minimumSalaryBasedOnGender);
		}
	}
	
	@Test
	public void givenEmployeePayrollInDB_ShouldReturnMaximumSalaryBasedOnGender() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<Double> expectedMaximumSalary = new ArrayList();
		expectedMaximumSalary.add(10000000.00);
		expectedMaximumSalary.add(9000000.00);
		List<Double> maximumSalaryBasedOnGender = employeePayrollService.getMaximumSalaryBasedOnGender(IOService.DB_IO);
		if(maximumSalaryBasedOnGender.size() == 2) {
			Assert.assertEquals(expectedMaximumSalary, maximumSalaryBasedOnGender);
		}
	}
	
	@Test
	public void givenEmployeePayrollInDB_ShouldReturnCountOfBasedOnGender() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<Integer> expectedCountBasedOnGender = new ArrayList();
		expectedCountBasedOnGender.add(4);
		expectedCountBasedOnGender.add(6);
		List<Integer> countBasedOnGender = employeePayrollService.getCountOfEmployeesBasedOnGender(IOService.DB_IO);
		if(countBasedOnGender.size() == 2) {
			Assert.assertEquals(expectedCountBasedOnGender, countBasedOnGender);
		}
	}
	
	@Test
	public void givenStartDateRange_WhenMatchesUsingPreparedStatement_ShouldReturnEmployeeDetails() {
		
		String startDate = "2013-01-01";
		String endDate = "2021-01-01";
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.getEmployeeDetailsBasedOnStartDateUsingPreparedStatement(IOService.DB_IO, startDate, endDate);
		Assert.assertEquals(6, employeePayrollData.size());
	}
	
	@Test
	public void givenNewEmployee_WhenAdded_ShouldSyncWithDB() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		employeePayrollService.addEmployeeToPayroll(11, "Mark", 5000000.00, 1234567890, LocalDate.now(), "M", 1);
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		Assert.assertEquals(10, employeePayrollData.size());
	}
	
	@Test
	public void givenNewEmployee_WhenAdded_ShouldSyncWithUpdatedDB() {
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		employeePayrollService.addEmployeeToUpdatedDatabse(12, "Macy", 6000000.00, 1334567890, LocalDate.now(), "F", 2);
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		Assert.assertEquals(11, employeePayrollData.size());
		
	}
	
	@Test
	public void givenEmployeePayroll_WhenDeleteRecord_ShouldeSyncWithDB(){
		
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		employeePayrollService.deleteEmployeeToPayroll("Macy");
		int result = employeePayrollService.checkedRecordDeletedFromDatabase("Macy");
		Assert.assertEquals(0, result);
	}
	
}