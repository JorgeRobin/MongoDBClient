package mongoDBClient;


public class StatusCode {
	public int status;
	public String error = "";
	public String cause = "";
	public String message = "";
	public String stacktrace = "";
	
	public StatusCode (int status){
		this.status = status;
	}
	
	public StatusCode(  int status, 
						String error,
						String cause,
						String message,
						String stacktrace){
		this.status = status;
		this.error = error;
		this.cause = cause;
		this.message = message;
		this.stacktrace = stacktrace;
	}
	@Override
	public String toString() {
	    return String.format("{\"status\": %s, \"error\": \"%s\", \"cause\": \"%s\", \"message\": \"%s\" }", 
	                    this.status,
	                    this.error,
	                    this.cause,
	                    this.message
	                );
	  }
}
