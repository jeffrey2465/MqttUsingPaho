package mqttusingpaho;
import java.nio.charset.Charset;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class PubClient {
	private String mBrokerURL; //#BrokerURL: "tcp://brokerIP:brokerPort"
	private String mClientID; //#mClientID: Client(publisher, subscriber를 구분짓는 고유값)
	private MqttClient mMqttClient; //#MqttClient: broker에 연결, 메세지 전송 및 수신등을 담당하는 클래스
	
	public PubClient(String brokerURL, String clientID) {		
		this.mBrokerURL = brokerURL;
		this.mClientID = clientID;
		init();
	}
	
	/* =============================================
	 * MqttClient 객체 생성 + 연결옵션 지정 + Broker 연결
	 */
	private void init() {
		
		try {
			//#1. MqttClient 객체 생성 ---------------------------------
			mMqttClient = new MqttClient(mBrokerURL, mClientID);
			//#2. Broker 연결 옵션 객체 생성 -----------------------------
			MqttConnectOptions options = new MqttConnectOptions();
			options.setCleanSession(true);
			options.setAutomaticReconnect(true);
			//#3. 연결옵션을 사용하여 Broker에 연결 ------------------------
			mMqttClient.connect(options); //연결옵션을 사용하지 않는 경우 매개변수 없이 사용			
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}
	
	/* =========================================
	 * MqttMessage 객체로 메세지 패키징 + publishing
	 */	
	public boolean sendMessage(String topic, String message) {		
		try {
			//#1.보내고자 하는 메시지를 MqttMessage로 패키징 -----------------
			MqttMessage mqttMessage = new MqttMessage(message.getBytes(Charset.defaultCharset())); //수신측 문자셋에 맞추어 수정
			//#2.MqttClient 객체의 publish() 메서드를 이용하여 메시지 전송 ----
			mMqttClient.publish(topic, mqttMessage);			
			return true;
		} catch (MqttException e) {
			e.printStackTrace();
		}		
		return false;
	}
	
	/* =========================================
	 * 접속해제 및 객체 반납
	 */
	public void close() {		
			try {
				if(mMqttClient.isConnected())
					mMqttClient.disconnect();
				mMqttClient.close();
			} catch (MqttException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public static void main(String[] args) throws InterruptedException {				
		//#1. MQTT 클라이언트 객체 생성
		PubClient pubClient = 
				new PubClient("tcp://192.168.0.60", MqttClient.generateClientId()); //MqttClient.generateClientId(): clientID 자동생성
		for(int i=0; i<5; i++) {
			pubClient.sendMessage("java", "안녕");
			Thread.sleep(1000);
		}
		
		//#2. 전송 후 연결 종료
		pubClient.close();
	
	}


}
