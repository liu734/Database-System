package heap;
import chainexception.*;


public class InvalidUpdateException extends ChainException {
  
  public InvalidUpdateException(Exception ex, String name)
    { 
      super(ex, name); 
    }
  
}




