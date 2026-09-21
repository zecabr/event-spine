# event-spine

[![CI](https://github.com/zecabr/event-spine/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/zecabr/event-spine/actions/workflows/ci.yml)
[![.NET 9](https://img.shields.io/badge/.NET-9.0-512BD4)](https://dotnet.microsoft.com/download)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)

**Backbone de eventos com outbox transacional, idempotência por chave, DLQ e projeções materializadas — implementação de referência em .NET 9.**

> ⚠️ Em desenvolvimento. Este README descreve o escopo do v0.1; código funcional chega bloco a bloco. Ver [Issue #1 Design v0](https://github.com/zecabr/event-spine/issues/1).

## Problema

Todo arquiteto sênior é cobrado, em entrevista e em revisão de design, a defender os padrões clássicos de sistemas distribuídos: **outbox transacional**, **idempotência por chave**, **DLQ observável**, **projeções materializadas**. Existe muito tutorial pela metade, pouco projeto público que mostre os quatro juntos, escritos com decisão explicada.

`event-spine` é essa implementação de referência. Não substitui Kafka em produção — é referência de _padrão_ sobre um domínio didático (Orders / e-commerce), com decisões arquiteturais documentadas em ADRs.

## O que é (v0.1)

- **Serviço A (Producer)** — API HTTP + outbox transacional em Postgres, com relay assíncrono que publica no Redpanda (Kafka-compat) usando `key = order_id` (ordem por agregado).
- **Serviço B (Consumer)** — worker que consome, aplica **idempotência por `event_id`** via inbox pattern, e alimenta a projeção materializada `orders_view`.
- **DLQ** — tópico separado + tabela + dashboard mínimo: por que a mensagem morreu, contadores por motivo, retry manual.
- **Testes de contrato** (schema registry) + **chaos test** (kill do consumer no meio de um lote via `docker kill`).
- **Runbook operacional** — o que fazer quando cada peça quebra.

## O que NÃO é

- Substituto de Kafka em produção — é referência de padrão, não benchmark.
- Framework de event-sourcing completo (sem snapshot, sem upcasting, sem replay complexo).
- Sistema de e-commerce — Orders é só o domínio didático que ilustra os padrões.

## Arquitetura

Ver [`docs/architecture.md`](docs/architecture.md) — C4 nível 1 (contexto) + nível 2 (containers) com fluxo end-to-end e fluxo de falha.

**ADRs em [`docs/adr/`](docs/adr/):**

- ✅ [ADR-001 — Outbox transacional vs. dual write](docs/adr/001-outbox-vs-dual-write.md).
- 🟡 ADR-002 — Escolha do broker (Redpanda vs. Kafka vs. RabbitMQ) *(antes do release v0.1)*.
- 🟡 ADR-003 — Política de retry e critério de envio pra DLQ *(antes do release v0.1)*.

## Quick start

Meta: **≤ 5 comandos** entre `git clone` e primeiro evento fluindo end-to-end.

```bash
git clone https://github.com/zecabr/event-spine && cd event-spine
docker compose up -d                                      # postgres + redpanda + console
dotnet run --project src/EventSpine.Producer &            # producer API + relay
dotnet run --project src/EventSpine.Consumer &            # consumer + projection + DLQ
curl -X POST http://localhost:5001/orders -d '{"amount":100}' -H 'Content-Type: application/json'
# → Order aparece na projeção materializada em ~200 ms
```

Redpanda Console em `http://localhost:8080` pra inspecionar tópicos e mensagens.

## Roadmap v0.1

Blocos escopados, um deliverable observável cada. Ordem sujeita a ajuste.

- ✅ **Bloco 1** — Fundação: solution, docker-compose, arquitetura, ADR-001, CI.
- ✅ **Bloco 2** — Outbox transacional: Producer API + relay funcional + Testcontainers end-to-end.
- 🟡 **Bloco 3** — Consumer idempotente + projeção materializada + Testcontainers.
- 🟡 **Bloco 4** — DLQ (topic + tabela + dashboard) + política de retry (ADR-003).
- 🟡 **Bloco 5** — Schema registry + testes de contrato (ADR-002 formalizada).
- 🟡 **Bloco 6** — Chaos test (`docker kill` do consumer) + runbook operacional.
- 🟡 **Bloco 7** — Release v0.1 pública.

## Stack

.NET 9 · ASP.NET Core minimal APIs · Entity Framework Core + Npgsql · Postgres 16 · Redpanda (Kafka-compat) via Confluent.Kafka · OpenTelemetry · xUnit · Testcontainers · docker-compose · GitHub Actions.

## Status

Em desenvolvimento (Blocos 1 e 2 concluídos). Repo público desde o primeiro commit — construção em aberto. Contribuições e feedback via issues bem-vindos.

## Autor

Zeca — [zecabr.github.io](https://zecabr.github.io). Um artigo técnico associado (*"Outbox pattern na prática — o que os tutoriais não te contam"*) sai junto com o release v0.1.

## Licença

MIT.
