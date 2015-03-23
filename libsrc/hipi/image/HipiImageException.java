package hipi.image;

public class HipiImageException extends RuntimeException {

  private Throwable cause;

  /**
   * Constructs a JSONException with an explanatory message.
   *
   * @param message
   *            Detail about the reason for the exception.
   */
  public HipiImageException(String message) {
    super(message);
  }
  
  /**
   * Constructs a new JSONException with the specified cause.
   * @param cause The cause.
   */
  public HipiImageException(Throwable cause) {
    super(cause.getMessage());
    this.cause = cause;
  }
  
  /**
   * Returns the cause of this exception or null if the cause is nonexistent
   * or unknown.
   *
   * @return the cause of this exception or null if the cause is nonexistent
   *          or unknown.
   */
  @Override
  public Throwable getCause() {
    return this.cause;
  }

}
