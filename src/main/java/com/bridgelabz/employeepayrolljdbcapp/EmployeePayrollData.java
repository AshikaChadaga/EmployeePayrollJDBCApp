package com.bridgelabz.employeepayrolljdbcapp;

import java.time.LocalDate;

public class EmployeePayrollData {
	public int employeeId;
	public String employeeName;
	public double employeeSalary;
	public LocalDate startDate;
	
	public EmployeePayrollData(Integer id, String name, Double salary, LocalDate date) {
		
		this.employeeId = id;
		this.employeeName = name;
		this.employeeSalary = salary;
		this.startDate = date;
	}
	
	@Override
	public String toString() {
		
		return "EmployeeId: "+employeeId+", EmployeeName: "+employeeName+", EmployeeSalary: "+employeeSalary;
	}
}
