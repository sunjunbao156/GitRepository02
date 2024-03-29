package top.doe.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import top.doe.spark.network.buffer.ManagedBuffer;
import top.doe.spark.network.buffer.NettyManagedBuffer;

import java.util.Objects;

public final class RpcResponse extends AbstractResponseMessage {
  public final long requestId;

  public RpcResponse(long requestId, ManagedBuffer message) {
    super(message, true);
    this.requestId = requestId;
  }

  @Override
  public Type type() { return Type.RpcResponse; }

  @Override
  public int encodedLength() {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    return 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    // See comment in encodedLength().
    buf.writeInt((int) body().size());
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new RpcFailure(requestId, error);
  }

  public static RpcResponse decode(ByteBuf buf) {
    long requestId = buf.readLong();
    // See comment in encodedLength().
    buf.readInt();
    return new RpcResponse(requestId, new NettyManagedBuffer(buf.retain()));
  }

  @Override
  public int hashCode() {
    return Objects.hash(requestId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcResponse) {
      RpcResponse o = (RpcResponse) other;
      return requestId == o.requestId && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("requestId", requestId)
      .append("body", body())
      .toString();
  }
}
