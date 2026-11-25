# Decisions tècniques pactades · Project 01

Acordades per l'Agus i la Clara el **25/11/2026**. Són els valors que tots dos fem servir al codi.
Si cal canviar-ne algun, primer es proposa en un issue i, quan els dos hi estem d'acord, s'actualitza aquest fitxer.

> ⚠️ Els llindars de la secció 1 **no els fixa l'enunciat**: els hem decidit nosaltres, i a l'informe hem de dir per què.

---

## 1. Llindars i valors per defecte

| Concepte | Valor pactat | Per què | On s'aplica |
|---|---|---|---|
| **Error de sensor** | `value == 100` exactament | Ho diu l'enunciat | P2 |
| **Sensor que necessita manteniment** | **> 30% de lectures errònies en un dia simulat** (errors / missatges rebuts del sensor) | Els errors normals del sensor ja són ~20% (`outlier_ratio = 0.2`). Un llindar per sota d'aquest valor faria saltar l'alerta a tots els sensors | P2 → P7 |
| **Sensor mort** | > 1 minut **real** sense **cap** missatge (`raw-readings`) | Ho diu l'enunciat. Un sensor que només envia 100 o missatges mal formats no està mort | P7 (punctuator `WALL_CLOCK_TIME`) |
| **Fora de rang segur** | temperatura **< 12 °C o > 30 °C** | Ho diu l'enunciat | P7 |
| **Mode comfort** | per defecte **20–25 °C**; l'usuari hi pot posar el seu `min`/`max` | Ho diu l'enunciat | P3/P4 |
| **Mode eco** | sempre **17–20 °C**; s'ignoren els `min`/`max` de l'usuari | L'enunciat només parla de rang triat per l'usuari al comfort | P3/P4 |
| **Mode away** | s'encén per sota de **12 °C** i s'apaga per sobre de **14 °C** | Antigel de l'enunciat, amb 2 °C de marge perquè la bomba no s'engegui i s'aturi a cada lectura | P3/P4 |
| **Usuari sense settings** | **comfort 20–25 °C** (`leftJoin`) | Si no, aquell usuari no rebria mai cap ordre | P4 |
| **Encendre / apagar** | s'encén si la temperatura < `min` i s'apaga si > `max`; entremig manté l'estat que tenia | Ho diu l'enunciat. Per això cal un state store | P4 |
| **Potència de cada bomba** | **2 kW** · 1 lectura = 1 minut simulat | Ho diu l'enunciat | P6 |
| **Tarifa** | nit 00–08 h **0,10 €/kWh** · dia 08–18 h i 22–24 h **0,15 €/kWh** · punta 18–22 h **0,25 €/kWh** | Ho diu l'enunciat (hora simulada) | P6 |
| **Minut dins del confort** | la temperatura és dins del rang `min`–`max` del mode actiu de l'usuari | Ho diu l'enunciat (P8) | P6 → P8 |

## 2. Temps

| Decisió | Valor |
|---|---|
| Rellotge de les finestres | **event time**: el `timestamp` del JSON, llegit amb un `TimestampExtractor` propi (ja és temps simulat) |
| Missatge sense `timestamp` vàlid | l'extractor torna el timestamp del registre de Kafka |
| `timestamp` sense decimals | s'ha d'acceptar (passa quan els microsegons són 0) |
| Grace de les finestres | **5 minuts simulats** |
| Hora simulada (P5) | finestra tumbling d'1 h de `timestamp` (≈ 1 minut real) |
| Dia simulat (P2, P6, P8) | de **00:00 a 24:00** del `timestamp` |
| Rellotges desfasats | cada sensor té el seu propi rellotge. És conegut i s'explica a l'informe |

## 3. Topics de Kafka

Tots els topics tenen **3 particions**, perquè els joins necessiten que els topics tinguin el mateix nombre de particions. Al nivell 3 es provaran 6, per demostrar l'escalat.

| Topic | Clau | Valor | Escriu → Llegeix | Notes |
|---|---|---|---|---|
| `raw-readings` | `user/room` | lectura tal com arriba | ingestor → P2, storage | |
| `user-settings` | `user` | `{"mode", "min"?, "max"?}` | ingestor → P3 | **compactat** |
| `clean-readings` | `user/room` | lectura sense errors | P2 → P4, P5, P6, P7, storage | |
| `sensor-health` | `user/room` | `{"day", "errors", "total", "error_ratio"}` | P2 → P7, storage | |
| `heatpump-commands` | `user/room` | `{"status": 0\|1}` | P4 → commander, P6 (KTable) | |
| `home-stats` | `user` | `{"window_start", "mean", "min", "max"}` | P5 → storage | |
| `energy-bill` | `user` | `{"day", "minutes_on", "kwh", "cost_eur"}` | P6 → P8, storage | |
| `daily-comfort` | `user` | `{"day", "minutes_total", "minutes_in_range", "comfort_pct"}` | P6 → P8, storage | el calcula la P6 (vegeu la secció 4) |
| `alerts` | `user` | vegeu la secció 5 | P7 → commander, storage | |
| `leaderboard` | `"all"` (clau fixa) | `{"day", "by_cost": [...], "by_comfort": [...]}` | P8 → storage | clau única: coll d'ampolla que s'explica a l'informe |

## 4. Qui calcula què

| Decisió | Detall |
|---|---|
| **Estat de les bombes per a la P6** | la P6 llegeix `heatpump-commands` com una **KTable** (clau `user/room`) i hi fa un join amb `clean-readings`, que té la mateixa clau. Així no depèn del state store de la P4 |
| **% de confort per a la P8** | el calcula la **P6** (Agus) i el publica a `daily-comfort`. La **P8** (Clara) només ordena els resultats |
| **Llindar de manteniment** | la P2 publica `error_ratio` a `sensor-health`, i la P7 compara aquest valor amb el 30% |

## 5. Alertes

- **Topic de Kafka:** `alerts`, amb clau `user`.
- **Topic MQTT:** el commander les publica al cloud a `<user>/alerts`, i el gateway les reenvia a casa.
- **Valor:**
```json
{"type": "dead_sensor | out_of_range | needs_maintenance",
 "user": "user1", "room": "room1",
 "timestamp": "2026-11-25T10:00:00", "detail": "temperature 31.2 > 30"}
```

## 6. Projecte Java (Kafka Streams)

- **Maven** + **Java 17**. Un sol `application.id` i una sola topologia.
- **Una classe per pipeline**, amb un mètode `build(StreamsBuilder builder)`: `P2Clean`, `P3Settings`, `P4Actuate`…
- `App.java` construeix totes les pipelines, imprimeix `Topology.describe()` i arrenca l'aplicació.
- Paquet `model/` amb els POJO compartits (`Reading`, `Settings`, `Command`, `Alert`, …) i un `JsonSerde` comú.
- **La Clara** fa l'esquelet i el `model/`. Després, cadascú només toca les classes de les seves pipelines.

## 7. InfluxDB

- Un **measurement per topic**: `raw_readings`, `clean_readings`, `sensor_health`, `home_stats`, `energy_bill`, `daily_comfort`, `alerts`, `leaderboard`.
- **Tags:** `user` i `room`, quan el topic els té. **Fields:** la resta de valors numèrics.
- Temps del punt a InfluxDB: el `timestamp` del missatge o l'inici de la finestra.

## 8. Com treballem

- **Git:** una pull request per tasca, que l'altra persona revisa en un màxim de 24 h. Commits en anglès. Els contractes (aquest fitxer i la secció 4 d'`arquitectura.md`) es canvien només via issue.
- **Nivell 3:** la Clara comença el minikube **dissabte 28/11**, en paral·lel amb la resta.
- **Integració:** **dilluns 30/11** ho ajuntem tot i ho provem de punta a punta. Dimarts 1/12, informe i captures.
- **Informe:** cadascú escriu la secció dels seus components. Cadascú apunta les seves hores des d'ara, perquè l'enunciat demana posar-les a l'informe.
- **Presentació:** abans d'entregar, ens expliquem l'un a l'altre el que ha fet cadascú, perquè els professors ens poden preguntar de qualsevol part.
