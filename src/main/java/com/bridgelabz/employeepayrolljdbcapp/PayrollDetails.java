package com.bridgelabz.employeepayrolljdbcapp;

public class PayrollDetails {
	
	double basicSalary, deductions, taxablePay, tax, netPay;
	
	public PayrollDetails() {
	}

	public double getBasicSalary() {
		return basicSalary;
	}

	public double getDeductions() {
		return deductions;
	}

	public double getTaxablePay() {
		return taxablePay;
	}

	public double getTax() {
		return tax;
	}

	public double getNetPay() {
		return netPay;
	}

	public void setBasicSalary(double basicSalary) {
		this.basicSalary = basicSalary;
	}

	public void setDeductions(double deductions) {
		this.deductions = deductions;
	}

	public void setTaxablePay(double taxablePay) {
		this.taxablePay = taxablePay;
	}

	public void setTax(double tax) {
		this.tax = tax;
	}

	public void setNetPay(double netPay) {
		this.netPay = netPay;
	}
	
	
}
