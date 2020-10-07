package poc.kafka.entity;

import java.util.Date;

public class Message {

	private String city;
	private double tempreature;
	private Long time ;
	private Tempreature tempClass;
	
	
	public Long getTime() {
		return time;
	}
	public void setTime() {
		this.time = new Date().getTime();
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public double getTempreature() {
		return tempreature;
	}
	public void setTempreature(double tempreature) {
		this.tempreature = tempreature;
	}
	public Tempreature getTempClass() {
		return tempClass;
	}
	public void setTempClass(Tempreature tempClass) {
		this.tempClass = tempClass;
	}
	
}
