# Project 01 – IoT comfort platform (Computació Distribuïda, UdL)

Simulated IoT platform: temperature sensors in several homes send readings
through a gateway and MQTT to the cloud, where Kafka and a Kafka Streams
application clean, analyse and control the heat pumps. Results go to InfluxDB.

```
sensor -> home Mosquitto -> gateway -> cloud Mosquitto -> ingestor -> Kafka -> Kafka Streams (P2-P8)
                                                                          \-> storage -> InfluxDB
```

Full architecture (components, topics, keys, pipelines P1-P8, who does what):
[`docs/arquitectura.md`](docs/arquitectura.md) (Catalan).

## Structure

| Path | What it is | Owner |
|---|---|---|
| `user/user/sensor_temperature/` | Provided sensor simulator (**do not modify**) | given |
| `user/user/gateway/` | Home gateway: home MQTT <-> cloud MQTT | Agus |
| `user/user/home1.yml`, `home2.yml` | One docker compose per home (user1, user2) | Agus |
| `user/user/docker-compose.yml` | Original example compose (kept for reference) | given |
| `cloud/ingestor/` | Cloud MQTT -> Kafka (`raw-readings`, `user-settings`) | Agus |
| `cloud/docker-compose.test.yml` | Minimal cloud only to test gateway + ingestor | Agus |
| `docs/informe/main.tex` | Report (LaTeX, Catalan) | both |

Still to do (see the task list): cloud compose with InfluxDB/Grafana, storage
service, Kafka Streams app (P2-P8), commander, minikube.

## MQTT topic convention

| Broker | Topic | Content |
|---|---|---|
| home  | `<room>/temperature` | sensor reading |
| home  | `<room>/heatpump` | command for the sensor `{"status": 0\|1}` |
| home  | `settings` | user comfort settings |
| cloud | `<user>/<room>/temperature` | reading + `user`, `room` |
| cloud | `<user>/<room>/heatpump` | command from the commander |
| cloud | `<user>/settings` | settings forwarded by the gateway |

Kafka keys: `raw-readings` -> `<user>/<room>`, `user-settings` -> `<user>`.

## How to run (current part)

```bash
# once: shared network that plays the role of "the Internet"
docker network create global_net

# test cloud (from cloud/)
docker compose -f docker-compose.test.yml --project-name cloud up --build

# homes (from user/user/)
docker compose -f home1.yml --project-name user1 up --build
docker compose -f home2.yml --project-name user2 up --build

# readings reaching Kafka, with their key
docker exec -it cloud-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw-readings \
  --from-beginning --property print.key=true
```

## Unit tests (no broker needed)

```bash
cd user/user/gateway && python -m unittest -v
cd cloud/ingestor && python -m unittest -v
```
