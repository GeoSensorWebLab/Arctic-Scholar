package main;

import java.util.Date;
import java.util.TimerTask;

public class ScheduledTask extends TimerTask {
	
	Feeder fd= new Feeder();
	Date now;
	
	@Override
	public void run(){
		
		try{
			now = new Date();
			System.out.println(now);
			
			fd.feeder();
			
		}catch (Exception e){
			e.printStackTrace();
		}catch (Throwable e){
			e.printStackTrace();
		}
	}
}
