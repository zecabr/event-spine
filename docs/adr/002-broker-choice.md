# ADR-002 — Escolha do broker (Redpanda vs. Kafka vs. RabbitMQ)

**Status:** accepted
**Data:** 2026-09-22
**Autor:** Zeca

## Contexto

O `event-spine` precisa de um broker para transportar eventos entre o producer (que grava em outbox + relay) e o consumer (que idempotência + projeta). O broker é o transporte principal e a escolha impacta:

- **Dev experience** — quanto custa subir localmente pra testar o padrão outbox → consume → projection.
- **Ordem por chave** — obrigatório pro fluxo (ordem por `order_id` garante que `OrderPaid` chega depois de `OrderCreated`).
- **Semântica de entrega** — precisamos de "at-least-once" com idempotência downstream (que já fizemos no ADR-001).
- **Ecossistema .NET** — quais clients são maduros, quais têm suporte a acks, idempotence, etc.
- **Trajetória de aprendizado** — o projeto é implementação de referência; a escolha precisa refletir uma decisão defensável em entrevista/design review, não conveniência.

Três candidatos considerados: **Kafka** (Apache), **Redpanda**, **RabbitMQ**.

## Decisão

**Redpanda em desenvolvimento e testes, com Confluent.Kafka como driver client (Kafka wire protocol).**

Redpanda implementa o protocolo Kafka nativamente — mesmo cliente, mesmas APIs (produce, consume, offsets, transactions). Ganhamos as garantias e ecossistema Kafka sem a JVM, Zookeeper (ou KRaft mode complicado), e sem os 512MB baseline de memória.

Em produção, a decisão fica aberta: como a wire compat é bit-a-bit, migrar de Redpanda pro Kafka clássico (ou o contrário) é uma mudança de `bootstrap.servers`, não de código. Documentado como propriedade explícita.

## Consequências

**Positivas**

- **Startup < 3s em dev.** Um único binário Go, single container em docker-compose. Confluent Kafka + Zookeeper (ou 3 nós em KRaft) demora 20-40s pra ficar pronto — atrito diário no ciclo TDD.
- **Testcontainers first-class.** `RedpandaBuilder` sobe pra cada test class sem cerimonia. Kafka via Testcontainers exige mais tunning (zookeeper, listeners internos vs externos) e pode piscar em CI.
- **Zero coupling ao cliente.** `Confluent.Kafka` é a lib .NET oficial e é o cliente de referência pra qualquer broker Kafka-wire-compatible. Nenhuma linha de código muda entre Redpanda dev e Kafka prod (se migrar).
- **Console incluído.** Redpanda Console (porta 8080 no docker-compose) mostra tópicos, mensagens, consumer groups sem instalar Kafka UI separado — melhora a demo do projeto.
- **Storage layout mais simples.** Redpanda usa 1 diretório por partition, sem os múltiplos segments/indexes do Kafka — inspecionar estado com `ls` funciona.

**Negativas**

- **Ecossistema menor.** Kafka Connect, KStreams, ksqlDB, Schema Registry oficial da Confluent — todos são Kafka-first. Redpanda tem alternativas próprias mas menos maduras. Pro escopo v0.1 (outbox pattern de referência) isso não morde.
- **Menor base de suporte enterprise conhecido.** Kafka em produção em Netflix, LinkedIn, Uber é referência universal. Redpanda ainda está consolidando cases públicos. Se a decisão precisa passar por comitê de arquitetura de empresa muito conservadora, é friction real.
- **Community vs Enterprise features.** Alguns recursos avançados (tiered storage, WASM transforms) são pagos no Redpanda Enterprise. Kafka open-source cobre mais features nativamente.

**Neutras**

- **Semantica de partitioning é idêntica** — `key = order_id` → mesma partition → ordem preservada. Independente do broker.
- **Idempotent producer + acks=all** — ambos brokers suportam identicamente. Configuração no `Confluent.Kafka.ProducerConfig` é a mesma.
- **Consumer groups + manual commit** — mesma semântica. `EnableAutoCommit = false` + `StoreOffset` + `Commit` funciona nos dois.

## Alternativas rejeitadas

**Apache Kafka + Zookeeper (setup clássico).** Rejeitado por peso operacional em dev — 2 containers (broker + zookeeper) + tempo de boot longo. Aprender KRaft mode (sem Zookeeper) resolve parte disso mas ainda é mais complexo que Redpanda. Para o objetivo "projeto de referência que roda rápido no `docker compose up`", a diferença de dev experience importa.

**Apache Kafka + KRaft mode (sem Zookeeper, single node).** Melhor que a versão clássica, mas ainda mais pesado que Redpanda (JVM, tuning de heap). Aceitável em produção, sub-ideal em dev/CI.

**RabbitMQ (protocolo AMQP).** Rejeitado por razões arquiteturais, não por qualidade — RabbitMQ é excelente para o que faz. Mas:

- **Semantica de partitioning por chave não é nativa.** Consistent hash exchange existe mas é plugin, e a ordem entre mensagens da mesma chave não é garantida da mesma forma que Kafka. Isso quebra o padrão que o projeto ilustra.
- **Rebalance de consumer group não existe como no Kafka.** Precisaria de padrões diferentes (queue exclusiva por consumer, etc.) que não são o que o mercado chama de "streaming pattern".
- **Log persistence não é o modelo.** RabbitMQ apaga mensagens após ack por padrão. Replay via `POST /dlq/{id}/replay` funciona porque o outbox e a `dlq_events` guardam o payload — mas o modelo mental fica confuso ("por que um broker que apaga tudo?").

RabbitMQ brilha em RPC assíncrono, work queues e roteamento complexo. Não brilha como backbone de eventos ordenados por agregado. Escolher RabbitMQ aqui seria contar a história errada.

**NATS JetStream.** Considerado brevemente. Semantica interessante (streams + consumers + ack), performance excelente, single binary. Rejeitado porque o ecossistema .NET (NATS.Client) é menos difundido que Confluent.Kafka, e a maioria dos arquitetos sêniores da comunidade tem exposição maior a Kafka do que NATS. Como o projeto é referência de padrão *reconhecível*, Kafka wire wins.

**Cloud-managed (MSK, Confluent Cloud, EventHubs).** Fora de escopo v0.1 — projeto precisa rodar offline no laptop com `docker compose up -d`. Migração pra managed é uma decisão de deploy, ortogonal à decisão de tecnologia.

## Revisão

Reavaliar em qualquer um destes cenários:

- **Precisar de Kafka Connect / Debezium** integrado ao projeto — se v0.2 adotar CDC do Postgres (ver ADR-001 alternativas), Redpanda Connect é mais imaturo que Kafka Connect e pode forçar migração.
- **Schema Registry externo virar requisito** — ADR-005 do Bloco 5 formaliza schemas JSON embedded; se o volume ou variedade justificar Confluent Schema Registry, é mais natural com Kafka clássico do que Redpanda.
- **Enterprise procurement bloquear Redpanda** — empresas com tooling padronizado em Kafka Enterprise podem não aceitar Redpanda. Como o cliente é o mesmo, migração é troca de `bootstrap.servers`.
- **Necessidade de tiered storage** para retention longa (>30 dias) sem custo alto de disco local — Redpanda oferece como Enterprise; Kafka open-source tem KIP-405 (tiered storage) desde 3.6.
