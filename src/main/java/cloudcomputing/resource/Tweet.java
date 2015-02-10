package cloudcomputing.resource;

public class Tweet {
	private String date;
	private String idTweet;
	private String idUser;
	private String lang;
	private String coordinates;
	private String text;
	
	public Tweet() {
		this.date = "";
		this.idTweet = "";
		this.idUser = "";
		this.lang = "";
		this.coordinates = "";
		this.text = "";
	}
	
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getIdTweet() {
		return idTweet;
	}

	public void setIdTweet(String idTweet) {
		this.idTweet = idTweet;
	}

	public String getIdUser() {
		return idUser;
	}

	public void setIdUser(String idUser) {
		this.idUser = idUser;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public String getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(String coordinates) {
		this.coordinates = coordinates;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	
	public String toString() {
		return "Date : " + this.date + "\n\t" + 
				"IdTweet : " + this.idTweet + "\n\t" +
				"IdUser : " + this.idUser + "\n\t" +
				"Lang : " + this.lang + "\n\t" +
				"Coordinates : " + this.coordinates + "\n\t" +
				"Text : " + this.text;
	}
}
