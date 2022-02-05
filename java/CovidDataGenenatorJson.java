import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RandomArraysOfString {

	public static void main(String[] args) throws JsonProcessingException {
		String[] modes = { "Bus", "Train", "Flight" };
		Date startDate = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-YYYY");
		String today = dateFormat.format(startDate);
		List<TravellerData> travellerDataList = new ArrayList<>();
		for (int i = 0; i <= 10; i++) {
			Long j = ThreadLocalRandom.current().nextLong(111111111111L, 999999999999L);
			String mode = modes[ThreadLocalRandom.current().nextInt(0, 3)];
			String hh_mm = makeDoubleDigit(ThreadLocalRandom.current().nextInt(0, 23), "left") + ":"
					+ makeDoubleDigit(ThreadLocalRandom.current().nextInt(0, 59), "right");
			TravellerData travellerData = new TravellerData(j, today + " " + hh_mm, mode, "Uttar Pradesh");
			travellerDataList.add(travellerData);
		}
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(travellerDataList);
			System.out.println(json);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String makeDoubleDigit(int num, String side) {
		String number = Integer.toString(num);
		if (number.length() == 1 && side.equals("left")) {
			return "0" + number;
		} else if (number.length() == 1 && side.equals("right")) {
			return number + "0";
		}
		return number;
	}
}

class TravellerData {
	private long aadhar_no;
	private String travel_date;
	private String mode;
	private String state;

	public TravellerData(long aadhar_no, String travel_date, String mode, String state) {
		super();
		this.aadhar_no = aadhar_no;
		this.travel_date = travel_date;
		this.mode = mode;
		this.state = state;
	}
	
	public long getAadhar_no() {
		return aadhar_no;
	}

	public String getTravel_date() {
		return travel_date;
	}

	public String getMode() {
		return mode;
	}


	public String getState() {
		return state;
	}

	public TravellerData() {
		super();
	}
	
	@Override
	public String toString() {
		return "TravellerData [aadhar_no=" + aadhar_no + ", travel_date=" + travel_date + ", mode=" + mode + ", state="
				+ state + "]";
	}
}