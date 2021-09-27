package com.bridgelabz.employeepayrolljdbcapp;

import java.time.LocalDate;

public class EmployeePayrollData {
	public int employeeId;
	public String employeeName;
	public double employeeSalary;
	public LocalDate startDate;
	
	public EmployeePayrollData(Integer id, String name, Double salary) {
		
		this.employeeId = id;
		this.employeeName = name;
		this.employeeSalary = salary;
	}
	
	public EmployeePayrollData(Integer id, String name, Double salary, LocalDate startDate) {
		this(id,name,salary);
		this.startDate = startDate;
	}
	
	@Override
	public String toString() {
		
		return "EmployeeId: "+employeeId+", EmployeeName: "+employeeName+", EmployeeSalary: "+employeeSalary;
	}
	
	@Override
	public boolean equals(Object object) {
		if(this == object)
			return true;
		if(object == null || getClass() != object.getClass())
			return false;
		EmployeePayrollData that = (EmployeePayrollData) object;
		return employeeId == that.employeeId && Double.compare(that.employeeSalary,  employeeSalary) == 0 && employeeName.equals(that.employeeName);
	}
}
