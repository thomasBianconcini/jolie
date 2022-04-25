package jolie.net;

public class KafkaMessage {
	public byte[] body;

	public KafkaMessage( byte[] body ) {
		this.body = body;
	}
}
