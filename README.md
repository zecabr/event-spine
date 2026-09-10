# event-spine

**Backbone de eventos com outbox, idempotência, DLQ e projeções — implementação de referência em .NET 9.**

> ⚠️ Pré-lançamento. Este README descreve a promessa; código chega em v0.1.

## Problema

Todo arquiteto sênior é cobrado, em entrevista e em revisão de design, a defender os padrões clássicos de sistemas distribuídos: outbox transacional, idempotência por chave, DLQ observável, projeções materializadas. Existe muito tutorial pela metade, pouco projeto público que mostre os quatro juntos, escritos com decisão explicada.

`event-spine` é essa implementação de referência. Não substitui Kafka em produção — é referência de _padrão_.

## O que é

- **Serviço A** produz eventos com _outbox pattern_ em Postgres, com relay assíncrono para Redpanda (compatível com Kafka).
- **Serviço B** consome, aplica idempotência por chave e alimenta projeção materializada.
- **DLQ** com dashboard mínimo: por que a mensagem morreu, retry manual, contadores.
- **Testes de contrato** (schema registry) + **chaos test** (kill do consumer no meio de um lote).
- **Runbook operacional** documentando o que fazer quando cada peça quebra.

## O que NÃO é

- Substituto de Kafka em produção.
- Framework de event-sourcing completo (sem snapshot, sem upcasting, sem replay complexo).
- Referência de performance — o objetivo é _clareza de padrão_, não benchmark.

## Arquitetura

_Diagrama C4 (nível 1 e 2) publicado com a Issue #1 Design v0._

## Quick start

Adicionado no README oficial na primeira release. Meta: ≤ 5 comandos entre `git clone` e o primeiro evento fluindo end-to-end no docker-compose.

## Roadmap curto

- **v0.1:** outbox transacional + consumer idempotente + DLQ com dashboard mínimo + runbook operacional. Uma release, escopo pequeno.
- **v0.2+:** backlog público construído a partir de uso real. Não prometido antecipadamente.

## Decisões arquiteturais

Publicadas em `docs/adr/` conforme forem tomadas:

- **ADR-001** — Outbox pattern vs. dual write: por que outbox venceu para este caso.
- **ADR-002** — Redpanda vs. Kafka vs. RabbitMQ como broker de referência.
- **ADR-003** — Política de retry e critério de envio para DLQ.

## Stack

.NET 9 · Postgres · Redpanda · OpenTelemetry · Testcontainers · docker-compose.

## Status

Pré-release, em desenho. Repo aberto ao público na primeira release v0.1. Contribuições bem-vindas a partir daí.

## Autor

Zeca — [zecabr.github.io](https://zecabr.github.io). Um artigo técnico associado (*"Outbox pattern na prática — o que os tutoriais não te contam"*) sai junto com a v0.1.

## Licença

MIT.
