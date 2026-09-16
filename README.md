# Udemy-Kafka-for-beginners

# Course details:
Course link: https://www.udemy.com/course/apache-kafka/?couponCode=MT260907G2

Course instructor: Stephane Maarek 

Helping platform: https://www.conduktor.io/apache-kafka-for-beginners


# Section 4 - Kafka Theory

**Apache Kafka** este o platformă distribuită de streaming de evenimente (*event streaming platform*) și un jurnal de completare distribuit (*distributed commit log*), proiectată pentru a gestiona fluxuri de date în timp real, la scară masivă, fiind recunoscută pentru debitul său extrem de ridicat (*high-throughput*) și toleranța la defecțiuni.

---

## 1. Arhitectura de Bază și Concepte Cheie

În esență, Kafka funcționează ca un sistem de mesagerie publicare-abonare (*publish-subscribe*), dar cu garanții stricte de persistență și ordine a datelor.

````
graph TD
    P1[Producer 1] -->|Trimite Mesaje| T1[Topic: Partitia 0]
    P2[Producer 2] -->|Trimite Mesaje| T2[Topic: Partitia 1]
    subgraph Kafka Cluster (Brokers)
        T1
        T2
    end
    T1 -->|Consumă Date| C1[Consumer Group A - C1]
    T2 -->|Consumă Date| C2[Consumer Group A - C2]
````

### Componente Principale:
*   **Eveniment / Mesaj (Event/Message):** Unitatea fundamentală de date din Kafka. Este compus dintr-o cheie (*key*), un text/valoare (*value*), un marcaj de timp (*timestamp*) și metadate opționale.
*   **Topic:** O categorie sau un nume de flux în care sunt stocate mesajele. Topicele sunt multi-producer și multi-consumer.
*   **Partiție (Partition):** Topicele sunt împărțite în bucăți mai mici numite partiții, distribuite pe mai multe noduri. **Ordinea mesajelor este garantată strict doar la nivel de partiție.**
*   **Offset:** Un identificator numeric secvențial atribuit fiecărui mesaj primit într-o partiție. Mesajele sunt imutabile și se adaugă doar la sfârșitul logului (*append-only*).
*   **Broker:** Un server individual din clusterul Kafka. Acesta primește mesaje de la producători, le scrie pe disc și le servește consumatorilor.

---

## 2. Producători și Consumatori

### Producători (Producers)
Aplicațiile care publică date în topicele Kafka. Producătorul decide în ce partiție trimite mesajul pe baza unei strategii:
*   **Cu cheie (Keyed):** Mesajele cu aceeași cheie ajung întotdeauna în aceeași partiție (folosind un algoritm de hashing), asigurând ordinea logică a evenimentelor.
*   **Fără cheie (Key-less):** Mesajele sunt distribuite prin tehnica *Round-Robin* sau prin logica internă a clientului (ex. Sticky Partitioner) pentru a echilibra încărcarea.

### Consumatori și Grupuri de Consumatori (Consumers & Consumer Groups)
Aplicațiile care citesc mesajele din topice.
*   **Grup de Consumatori (Consumer Group):** O colecție de consumatori care cooperează pentru a citi dintr-un topic. 
*   Fiecare partiție dintr-un topic este alocată unui **singur consumator** din cadrul grupului. Dacă numărul de consumatori depășește numărul de partiții, consumatorii extra vor rămâne inactivi (inactivity/idle).
*   **Rebalansare (Rebalancing):** Procesul prin care Kafka redistribuie partițiile atunci când un consumator părăsește sau se alătură grupului.

---

## 3. Replicare, Disponibilitate și Consistență

Pentru a asigura toleranța la defecțiuni, fiecare partiție are replici pe mai mulți brokeri.

*   **Leader:** Replică principală care gestionează toate cererile de citire și scriere pentru partiția respectivă.
*   **Follower:** Replici secundare care doar copiază datele de pe lider pentru a rămâne sincronizate.
*   **ISR (In-Sync Replicas):** Lista de replici (followers) care sunt complet la zi cu liderul. Dacă liderul pică, un nou lider este ales doar din rândul membrilor ISR.

### Politici de Confirmare (Acks)
Producătorul poate configura nivelul de siguranță al scrierii prin parametrul `acks`:
1.  `acks=0`: Producătorul nu așteaptă nicio confirmare. Risc mare de pierdere a datelor, dar viteză maximă.
2.  `acks=1`: Producătorul așteaptă confirmarea doar de la lider. Datele se pot pierde dacă liderul pică înainte de replicare.
3.  `acks=all` (sau `-1`): Producătorul așteaptă ca toate replicile din ISR să confirme scrierea. Oferă cea mai mare siguranță a datelor.

---

## 4. Garanții de Livrare (Delivery Guarantees)

Kafka suportă trei tipuri de semantici în procesarea datelor:

| Semantică | Descriere | Detalii Tehnice |
| :--- | :--- | :--- |
| **At-most-once** | Mesajele pot fi pierdute, dar niciodată duplicate. | Consumatorul salvează offset-ul *înainte* de a procesa mesajul. |
| **At-least-once** | Mesajele nu se pierd, dar pot apărea duplicate. | Consumatorul salvează offset-ul *după* procesarea cu succes a mesajului. |
| **Exactly-once (EOS)** | Fiecare mesaj este procesat o singură dată. | Realizat prin Idempotență la Producător și suport pentru Tranzacții Kafka. |

---

## 5. Managementul Clusterului: De la ZooKeeper la KRaft

Istoric, Kafka se baza pe **Apache ZooKeeper** pentru managementul metadatelor din cluster (starea brokerilor, coordonarea liderilor, configurări).

*   **Problema cu ZooKeeper:** Limita scalabilitatea. Sincronizarea metadatelor la mii de partiții introducea latențe mari în caz de defecțiuni.
*   **Soluția Modernă (KRaft):** *Kafka Raft Metadata Mode* elimină complet dependența de ZooKeeper. Metadatele sunt acum stocate direct în interiorul Kafka, într-un topic specializat, utilizând algoritmul de consens Raft. Acest lucru permite clusterelor să scaleze la milioane de partiții și reduce dramatic timpul de failover.

---

## 6. De ce este Kafka atât de rapid?

Performanța remarcabilă a Kafka se datorează unor decizii inteligente de inginerie hardware și software:
1.  **I/O Secvențial pe Disc (Sequential Disk I/O):** Kafka scrie datele liniar la capătul logului. Accesul secvențial pe hard-disk-uri moderne sau SSD-uri este la fel de rapid ca accesul la memoria RAM.
2.  **Zero-Copy:** Datele sunt transferate direct din cache-ul paginii sistemului de operare (*Page Cache*) în socket-ul de rețea, ocolind spațiul aplicației (JVM). Astfel se elimină context-switching-ul și copierea redundantă în memorie.
3.  **Gruparea Mesajelor (Batching):** Producătorii și consumatorii trimit și citesc mesaje în pachete masive, reducând overhead-ul protocolului de rețea.

## Section 5 - Starting Kafka

Pentru asta exista ghiduri separate in functie de sistemul de operare: https://www.conduktor.io/kafka/starting-kafka

# Section 7 - Kafka CLI 101

 Kafka CLI command reference for local development.

 **Kafka broker:** `localhost:9092`

 ## Prerequisites

 Make sure Kafka is running and accessible on `localhost:9092`.

 Optionally define the broker as an environment variable:

```
export KAFKA_BROKER=localhost:9092
```

 The examples below assume Kafka CLI scripts are available under `bin/`.

---

 ## Topics

 ### List Topics

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

 ### Create a Topic

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic my-topic
```

 Create a topic with 3 partitions:

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 1
```

 > For a single local Kafka broker, use `--replication-factor 1`.

 ### Describe a Topic

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic my-topic
```

 ### Delete a Topic

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic my-topic
```

 ### Increase Number of Partitions

 Increase `my-topic` to 6 partitions:

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic my-topic \
  --partitions 6
```

 > Kafka allows increasing the number of partitions, but not decreasing them.

---

 ## Producing Messages

 ### Start a Console Producer

```
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

 Enter messages interactively:

```
Hello Kafka
First message
Second message
```

 Press `Ctrl+C` to exit.

 ### Produce a Single Message

```
echo "Hello Kafka" | \
  bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

 ### Produce Key/Value Messages

 Use `:` as the key/value separator:

```
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --property "parse.key=true" \
  --property "key.separator=:"
```

 Example:

```
user-1:Hello
user-2:Hello Kafka
user-3:Another message
```

---

 ## Consuming Messages

 ### Start a Console Consumer

```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

 The consumer waits for new messages.

 ### Consume From the Beginning

```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning
```

 ### Consume a Limited Number of Messages

 Read 10 messages and exit:

```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning \
  --max-messages 10
```

 ### Consume From a Specific Partition

```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partition 0 \
  --from-beginning
```

 ### Consume Messages With Keys

```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" : "
```

 Example output:

```
user-1 : Hello
user-2 : Hello Kafka
```

---

 ## Consumer Groups

 ### List Consumer Groups

```
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

 ### Describe a Consumer Group

```
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group my-group
```

 This displays information such as:

 - Topic
- Partition
- Current offset
- Log end offset
- Consumer lag
- Consumer ID
- Host

 ### Consume Using a Consumer Group

```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --group my-group
```

 Start multiple consumers using the same group ID:

```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --group my-group
```

 Kafka distributes partitions between consumers in the same group.

---

 ## Consumer Offset Management

 ### Reset to the Earliest Offset

 Preview the reset:

```
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group my-group \
  --topic my-topic \
  --reset-offsets \
  --to-earliest
```

 Execute the reset:

```
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group my-group \
  --topic my-topic \
  --reset-offsets \
  --to-earliest \
  --execute
```

 Other useful reset options:

```
--to-earliest
--to-latest
--to-offset <offset>
--shift-by <number>
--to-datetime <datetime>
```

---

 ## Offsets

 ### Get Topic Offsets

```
bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

 ### Get Offsets for a Specific Partition

```
bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partition 0
```

---

 ## Topic Configuration

 ### View Topic Configuration

```
bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic \
  --describe
```

 ### Change Topic Retention

 Set the retention period to 1 hour:

```
bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic \
  --alter \
  --add-config retention.ms=3600000
```

 ### Remove Custom Retention Configuration

```
bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic \
  --alter \
  --delete-config retention.ms
```

---

 ## Common Development Workflow

 ### 1\. Create a Topic

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-topic \
  --partitions 1 \
  --replication-factor 1
```

 ### 2\. Start a Consumer

 In one terminal:

```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning
```

 ### 3\. Start a Producer

 In another terminal:

```
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic
```

 ### 4\. Send Messages

```
message 1
message 2
message 3
```

 The consumer should receive the messages.

---

 ## Quick Reference

 | Operation | Command |
| --- | --- |
| List topics | `kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Create topic | `kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic` |
| Describe topic | `kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic my-topic` |
| Delete topic | `kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic my-topic` |
| Produce messages | `kafka-console-producer.sh --bootstrap-server localhost:9092 --topic my-topic` |
| Consume messages | `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic` |
| Consume from beginning | `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --from-beginning` |
| List consumer groups | `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list` |
| Describe consumer group | `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group` |
| Get offsets | `kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic my-topic` |

---

 ## Using `KAFKA_BROKER`

 Set the broker once:

```
export KAFKA_BROKER=localhost:9092
```

 Then use:

```
bin/kafka-topics.sh \
  --bootstrap-server $KAFKA_BROKER \
  --list
```

 Producer:

```
bin/kafka-console-producer.sh \
  --bootstrap-server $KAFKA_BROKER \
  --topic my-topic
```

 Consumer:

```
bin/kafka-console-consumer.sh \
  --bootstrap-server $KAFKA_BROKER \
  --topic my-topic \
  --from-beginning
```

---

 ## Docker

 If Kafka is running in Docker and port `9092` is exposed to the host, the following works from the host:

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

 If running the Kafka CLI **inside the Kafka container**, the broker address may instead be:

```
kafka:9092
```

 The exact address depends on your Docker/Compose configuration.

---

 ## Notes

 - `localhost:9092` assumes Kafka is accessible from the machine where the CLI command is executed.
- For a single-broker local Kafka setup, use replication factor `1`.
- Topic partition counts can be increased but not decreased.
- Use `--from-beginning` when you want to inspect existing messages.
- Be careful with consumer-group offset resets because `--execute` changes the group's committed offsets.
- Topic deletion depends on the broker's topic-deletion configuration.