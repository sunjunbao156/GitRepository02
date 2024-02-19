package top.doe.spark.network.protocol;

import top.doe.spark.network.buffer.ManagedBuffer;

public abstract class AbstractResponseMessage extends AbstractMessage implements ResponseMessage {

  protected AbstractResponseMessage(ManagedBuffer body, boolean isBodyInFrame) {
    super(body, isBodyInFrame);
  }

  public abstract ResponseMessage createFailureResponse(String error);
}