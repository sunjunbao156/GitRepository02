package top.doe.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import top.doe.spark.network.buffer.ManagedBuffer;
import top.doe.spark.network.buffer.NettyManagedBuffer;


public final class OneWayMessage extends AbstractMessage implements RequestMessage {

  public OneWayMessage(ManagedBuffer body) {
    super(body, true);
  }

  @Override
  public Type type() {
    return Type.OneWayMessage;
  }

  @Override
  public int encodedLength() {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    return 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    // See comment in encodedLength().
    buf.writeInt((int) body().size());
  }

  public static OneWayMessage decode(ByteBuf buf) {
    // See comment in encodedLength().
    buf.readInt();
    return new OneWayMessage(new NettyManagedBuffer(buf.retain()));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OneWayMessage) {
      OneWayMessage o = (OneWayMessage) other;
      return super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("body", body())
      .toString();
  }
}
