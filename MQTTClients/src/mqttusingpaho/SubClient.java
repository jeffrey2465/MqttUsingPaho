package mqttusingpaho;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class SubClient {
	private String mBrokerURL; //#BrokerURL: "tcp://brokerIP:brokerPort"
	private String mClientID; //#mClientID: Client(publisher, subscriber를 구분짓는 고유값)
	private MqttClient mMqttClient; //#MqttClient: broker에 연결, 메세지 전송 및 수신등을 담당하는 클래스
		
	public SubClient(String brokerURL, String clientID) {
		this.mBrokerURL = brokerURL;
		this.mClientID = clientID;
		init();
		setCallback();
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
	
	private void setCallback() {
		//#1. MqttCallback 객체 생성 ------------------------------
		MqttCallback callback = new MqttCallback() {
			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				// 메시지가 도착
				System.out.println(topic + " : 메시지가 도착하였습니다. : " + new String(message.getPayload()));				
			}			
			@Override
			public void deliveryComplete(IMqttDeliveryToken arg0) {
				// 메시지 전달이 완료되었을때 호출
				System.out.println("메시지전달이 완료되었습니다.");
			}			
			@Override
			public void connectionLost(Throwable arg0) {
				// 연결이 끊어졌을대 호출
				System.out.println("연결이 끊어졌습니다.");
			}
		};		
		
		//#2. mMqttClient에 콜백 연결
		mMqttClient.setCallback(callback);		
	}
	
	/*
	 * topic별 메시지 수진대기 (하나의 topic, 여러개의 topic 수신으로 overriding)
	 */	
	public void receiveMessage(String topic) {
		//#1. topic에 대한 수신 대기
		try {
			mMqttClient.subscribe(topic);
		} catch (MqttException e) {		
			e.printStackTrace();
		}
	}	
	public void receiveMessage(String[] topics) {
		//#1. topic들에 대한 수신 대기
		try {
			mMqttClient.subscribe(topics);
		} catch (MqttException e) {
			e.printStackTrace();
		}
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
		SubClient subClient = 
					new SubClient("tcp://192.168.0.60", MqttClient.generateClientId()); //MqttClient.generateClientId(): clientID 자동생성
		subClient.receiveMessage("java");
		
		//#2. 10초 뒤 연결종료
		Thread.sleep(10000);
		subClient.close(); 
		
	}
}
