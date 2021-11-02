# Author: Dominik Guzowski, 19334866

port=12345
topic=TEMP
freq=5000
brokerip=localhost
brokers=@
.SILENT:

# Port is the port on which the broker will be listening on.
# List of brokers is a list of other brokers in the system, to be given in the following format: B1_address:B1_port@B2_address:B2_port@...
broker:
	javac ./src/BrokerServer.java -d ./bin/
	java -cp ./bin src.BrokerServer $(port) $(brokers)

# Port is the listening port of the broker to which the dashboard will be subscribing to.
# Broker IP is the IP Address of the broker to which the dashboard will be subscribing to.
dashboard:
	javac ./src/Dashboard.java -d ./bin/
	java -cp ./bin src.Dashboard $(port) $(brokerip)

# Port is the listening port of the broker to which the actuator will be subscribing to for instructions and sending content.
# Topic is the topic for publishing content.
# Freq is the frequency of how often the content should be published in ms.
# Broker IP is the IP Address of the broker to which the dashboard will be subscribing to for instructions and sending content.
actuator:
	javac ./src/Actuator.java -d ./bin/
	java -cp ./bin src.Actuator $(port) $(topic) $(freq) $(brokerip)
