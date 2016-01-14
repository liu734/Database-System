package heap;
import chainexception.*;


public class SpaceNotAvailableException extends ChainException {
  
  public SpaceNotAvailableException(Exception ex, String name)
    { 
      super(ex, name); 
    }
  
}




