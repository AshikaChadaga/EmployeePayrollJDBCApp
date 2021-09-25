package com.bridgelabz.employeepayrolljdbcapp;

public class EmployeePayrollData {
	int employeeId;
	String employeeName;
	double employeeSalary;
	
	public EmployeePayrollData(Integer id, String name, Double salary) {
		
		this.employeeId = id;
		this.employeeName = name;
		this.employeeSalary = salary;
	}
	
	@Override
	public String toString() {
		
		return "EmployeeId: "+employeeId+", EmployeeName: "+employeeName+", EmployeeSalary: "+employeeSalary;
	}
}
