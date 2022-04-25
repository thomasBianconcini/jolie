package jolie.net;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.network.*;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class MyKafkaChannel implements AutoCloseable{
	private static final long MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS = 1000 * 1000 * 1000;
	private final String id;
	private final TransportLayer transportLayer;
	private long networkThreadTimeNanos;
	private final int maxReceiveSize;
	private final MemoryPool memoryPool;
	private final ChannelMetadataRegistry metadataRegistry;
	private NetworkReceive receive;
	private NetworkSend send;
	private boolean disconnected;
	private ChannelMuteState muteState;
	private ChannelState state;
	private SocketAddress remoteAddress;
	private boolean midWrite;
	public enum ChannelMuteState {
		NOT_MUTED,
		MUTED,
		MUTED_AND_RESPONSE_PENDING,
		MUTED_AND_THROTTLED,
		MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
	}
	public enum ChannelMuteEvent {
		REQUEST_RECEIVED,
		RESPONSE_SENT,
		THROTTLE_STARTED,
		THROTTLE_ENDED
	}
	public MyKafkaChannel(String id, TransportLayer transportLayer, int maxReceiveSize,MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry)
	{
		this.id = id;
		this.transportLayer = transportLayer;
		this.networkThreadTimeNanos = 0L;
		this.maxReceiveSize = maxReceiveSize;
		this.memoryPool = memoryPool;
		this.metadataRegistry = metadataRegistry;
		this.disconnected = false;
		this.muteState = ChannelMuteState.NOT_MUTED;
		this.state = ChannelState.NOT_CONNECTED;
	}
	public void prepare() throws IOException{
		boolean prep = false;
		try
		{
			if (!transportLayer.ready())
				transportLayer.handshake();
			else
				prep = true;


		}catch(Exception e)
		{
			String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
			// NOT-CONNECTED FROM AUTENTICATION-FAILED
			state = new ChannelState(ChannelState.State.NOT_CONNECTED, (AuthenticationException) e, remoteDesc);
			if(prep)
			{
				failCloseAll();

			}            throw e;
		}
		if (ready())
		{
			state= ChannelState.READY;
		}
	}

	private void failCloseAll() {
		transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
	}

	public boolean ready()
	{
		return transportLayer.ready();
	}
	public void disconnect()
	{
		disconnected= true;
		if((state == ChannelState.NOT_CONNECTED) && remoteAddress !=null)
		{
			state = new ChannelState(ChannelState.State.NOT_CONNECTED, remoteAddress.toString());
		}
		transportLayer.disconnect();
	}
	public void state(ChannelState state) {
		this.state = state;
	}
	public ChannelState state() {
		return this.state;
	}
	public boolean finishConnect() throws IOException {
		SocketChannel socketChannel= transportLayer.socketChannel();
		if(socketChannel !=null)
		{
			remoteAddress = socketChannel.getRemoteAddress();
		}
		boolean connected =  transportLayer.finishConnect();
		if(connected)
		{
			if(ready())
			{
				state = ChannelState.READY;
			}else if (remoteAddress != null) {
				state = new ChannelState(ChannelState.State.NOT_CONNECTED, remoteAddress.toString());
			}else
			{
				state=ChannelState.NOT_CONNECTED;
			}

		}
		return connected;

	}
	public boolean isConnected() {
		return transportLayer.isConnected();
	}
	public String id() {
		return id;
	}
	public SelectionKey selectionKey() {
		return transportLayer.selectionKey();
	}
	void mute() {
		if (muteState == ChannelMuteState.NOT_MUTED) {
			if (!disconnected) transportLayer.removeInterestOps(SelectionKey.OP_READ);
			muteState = ChannelMuteState.MUTED;
		}
	}
	boolean maybeUnmute() {
		if (muteState == ChannelMuteState.MUTED) {
			if (!disconnected) transportLayer.addInterestOps(SelectionKey.OP_READ);
			muteState = ChannelMuteState.NOT_MUTED;
		}
		return muteState == ChannelMuteState.NOT_MUTED;
	}
	public void handleChannelMuteEvent(ChannelMuteEvent event) {
		boolean stateChanged = false;
		switch (event) {
		case REQUEST_RECEIVED:
			if (muteState == ChannelMuteState.MUTED) {
				muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
				stateChanged = true;
			}
			break;
		case RESPONSE_SENT:
			if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
				muteState = ChannelMuteState.MUTED;
				stateChanged = true;
			}
			if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
				muteState = ChannelMuteState.MUTED_AND_THROTTLED;
				stateChanged = true;
			}
			break;
		case THROTTLE_STARTED:
			if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
				muteState = ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING;
				stateChanged = true;
			}
			break;
		case THROTTLE_ENDED:
			if (muteState == ChannelMuteState.MUTED_AND_THROTTLED) {
				muteState = ChannelMuteState.MUTED;
				stateChanged = true;
			}
			if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
				muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
				stateChanged = true;
			}
		}
		if (!stateChanged) {
			throw new IllegalStateException("Cannot transition from " + muteState.name() + " for " + event.name());
		}
	}
	public ChannelMuteState muteState() {
		return muteState;
	}
	public boolean isMuted() {
		return muteState != ChannelMuteState.NOT_MUTED;
	}
	public boolean isInMutableState() {
		if (receive == null || receive.memoryAllocated())
			return false;
		return transportLayer.ready();
	}
	public boolean hasSend() {
		return send != null;
	}
	public InetAddress socketAddress() {
		return transportLayer.socketChannel().socket().getInetAddress();
	}
	public String socketDescription() {
		Socket socket = transportLayer.socketChannel().socket();
		if (socket.getInetAddress() == null)
			return socket.getLocalAddress().toString();
		return socket.getInetAddress().toString();
	}
	public void setSend(NetworkSend send) {
		if (this.send != null)
			throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress, connection id is " + id);
		this.send = send;
		this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
	}
	public NetworkSend maybeCompleteSend() {
		if (send != null && send.completed()) {
			midWrite = false;
			transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
			NetworkSend result = send;
			send = null;
			return result;
		}
		return null;
	}
	public long read() throws IOException {
		if (receive == null) {
			receive = new NetworkReceive(maxReceiveSize, id, memoryPool);
		}
		long bytesReceived = receive(this.receive);

		if (this.receive.requiredMemoryAmountKnown() && !this.receive.memoryAllocated() && isInMutableState()) {
			//pool must be out of memory, mute ourselves.
			mute();
		}
		return bytesReceived;
	}
	public NetworkReceive currentReceive() {
		return receive;
	}
	public NetworkReceive maybeCompleteReceive() {
		if (receive != null && receive.complete()) {
			receive.payload().rewind();
			NetworkReceive result = receive;
			receive = null;
			return result;
		}
		return null;
	}
	public long write() throws IOException {
		if (send == null)
			return 0;

		midWrite = true;
		return send.writeTo(transportLayer);
	}
	public void addNetworkThreadTimeNanos(long nanos) {
		networkThreadTimeNanos += nanos;
	}
	public long getAndResetNetworkThreadTimeNanos() {
		long current = networkThreadTimeNanos;
		networkThreadTimeNanos = 0;
		return current;
	}
	private long receive(NetworkReceive receive) throws IOException {
		try {
			return receive.readFrom(transportLayer);
		} catch (Exception e) {
			String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
			state = new ChannelState(ChannelState.State.NOT_CONNECTED, (AuthenticationException) e, remoteDesc);
			throw e;
		}
	}

	public boolean hasBytesBuffered() {
		return transportLayer.hasBytesBuffered();
	}
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MyKafkaChannel that = (MyKafkaChannel) o;
		return id.equals(that.id);
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String toString() {
		return super.toString() + " id=" + id;
	}
	public ChannelMetadataRegistry channelMetadataRegistry() {
		return metadataRegistry;
	}
	@Override
	public void close() throws Exception {
		this.disconnected = true;
		Utils.closeAll(transportLayer,receive,metadataRegistry);
	}
}
