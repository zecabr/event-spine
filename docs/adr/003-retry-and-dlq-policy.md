# ADR-003 — Política de retry e critério de envio pra DLQ

**Status:** accepted
**Data:** 2026-09-21
**Autor:** Zeca

## Contexto

O consumer do `event-spine` (Serviço B) recebe eventos do tópico `orders.events` e aplica cada um na projeção materializada `orders_view`, com idempotência via `consumer_inbox` (ADR-001, Bloco 3). Nem toda falha é permanente: banco temporariamente indisponível, timeout de rede, deadlock de transação, restart do Postgres — situações onde a próxima tentativa provavelmente funciona.

Ao mesmo tempo, algumas falhas são "venenosas" — o evento em si está mal formado (JSON quebrado), ou viola invariante de domínio de forma que nenhuma retentativa vai resolver (`OrderPaid` pra order que não existe pode ser sinal de bug upstream). Ficar retentando indefinidamente esses eventos:

- Bloqueia o consumo do resto da partition (Kafka mantém ordem).
- Gera loops de log ruidosos sem valor operacional.
- Esconde o problema real — ninguém investiga se o alerta é "consumer travado" ao invés de "temos N eventos venenosos aguardando triagem".

A pergunta é: **quantas retentativas, e o que fazer quando esgotar?**

## Decisão

**Retry inline no consumer com N=3 tentativas + backoff exponencial (100 ms, 200 ms, 400 ms). Após a 3ª falha, o evento vai pra tabela `dlq_events` (fonte da verdade) e best-effort pro tópico `orders.events.dlq`. O offset é comitado — o consumer segue.**

Configuração via `RetryOptions`:

- `MaxAttempts = 3` — uma tentativa inicial + 2 retentativas.
- `BackoffBaseMs = 100` — nth retry espera `BackoffBaseMs * 2^(n-1)` ms.
- `DlqTopicName = "orders.events.dlq"` — tópico Kafka pra observers externos.
- `MainTopicName = "orders.events"` — usado pelo replay pra re-injetar no fluxo principal.

Eventos malformados (falha de deserialização) vão pro DLQ imediatamente, com `reason="poison"` e `attempts=0` — sem retry (não faz sentido re-parsear o mesmo JSON três vezes).

Eventos com falha na projection após esgotar retries vão com `reason="projection_failure"` e `attempts=3`.

**Tabela `dlq_events` é a fonte da verdade.** Publicação no tópico DLQ é best-effort — se o broker cair na hora, a tabela ainda capturou o evento. UNIQUE(event_id) garante idempotência: re-envio do mesmo event_id atualiza attempts + last_failed_at ao invés de duplicar.

**Endpoints operacionais:**

- `GET /dlq` — lista eventos não replayados, ordenados por `last_failed_at` desc.
- `GET /dlq/summary` — contadores por `reason` (visualização rápida de saúde).
- `GET /dlq/{id}` — detalhe de um evento (payload cru pra investigação).
- `POST /dlq/{id}/replay` — republica no `orders.events`, marca `replayed_at` + incrementa `replayed_count`.

## Consequências

**Positivas**

- **Consumer nunca trava por evento venenoso.** Retry finito + DLQ garante que o pipeline continua processando o próximo evento, mesmo com bugs em produção.
- **Observabilidade sem esforço adicional.** `SELECT reason, COUNT(*) FROM dlq_events WHERE replayed_at IS NULL GROUP BY reason` é o dashboard de saúde do sistema.
- **Recuperação manual controlada.** Operador triagia via `GET /dlq`, corrige a causa raiz (código, dado, config), replaya via `POST /dlq/{id}/replay`. O evento passa pela idempotência normal do inbox — replay dobrado não vira dado duplicado.
- **Idempotência do próprio DLQ.** UNIQUE(event_id) evita bagunça se o retry acabar publicando duplicado.
- **Testável sem broker.** `IKafkaTopicProducer` interface permite fake in-memory nos integration tests, mantendo cobertura do fluxo DLQ sem depender de Kafka rodando (evita flakiness).

**Negativas**

- **Retry inline segura a partition.** Enquanto os 3 attempts + backoffs rodam (~700 ms na pior sequência), o consumer não processa o próximo evento. Aceitável pra v0.1 — se virar problema, retry assíncrono via tabela de "pending_retry" é a evolução natural.
- **Backoff não distingue tipos de erro.** Deadlock de banco e "aggregate não existe" recebem o mesmo tratamento. Refinamento (retry só em exceptions específicas) fica pra quando os padrões de erro aparecerem em produção.
- **Retry state se perde no restart do consumer.** Se o consumer crashar entre a 2ª e a 3ª tentativa, o Kafka reentrega e o retry recomeça do zero — potencialmente 6 attempts totais em vez de 3. Aceitável: idempotência downstream (inbox) garante que reprocessamentos não corrompem dado. Documentar como comportamento esperado.
- **DLQ topic é best-effort.** Se a publicação Kafka falha (broker down), só a tabela captura. Observers externos que dependam do tópico podem perder eventos. Mitigação: usar a tabela como fonte primária pra alerta; tópico é conveniência.

**Neutras**

- Payload no `dlq_events.payload` é armazenado como TEXT (JSON serializado). Tamanho médio esperado ~500 bytes; se algum evento futuro tiver payload muito grande, considerar compressão ou storage externo (S3 + ref no dlq).
- `replayed_count` permite auditar quantas vezes um evento foi replayado — útil pra investigar loops onde a mesma correção é tentada várias vezes sem sucesso.

## Alternativas rejeitadas

**Retry infinito com backoff exponencial (sem DLQ).** Rejeitada porque um evento genuinamente venenoso trava a partition indefinidamente. Alertas de "consumer atrasado" não dizem *por que* — obriga investigação em logs, quando uma tabela DLQ diz na hora.

**Sem retry — mandar direto pra DLQ na primeira falha.** Rejeitada porque desperdiça a resolução simples de problemas transientes (banco reiniciando, timeout de rede). 3 tentativas custa <1s e resolve a maioria dos casos operacionais reais.

**Retry assíncrono via tabela `pending_retry`.** Consumer marca falha na tabela e um scheduler separado tenta de novo. Rejeitada pra v0.1 por adicionar um componente (scheduler) sem ganho proporcional — v0.1 é implementação de referência, não high-scale. Se retry sync virar gargalo em uso real, essa é a evolução natural.

**DLQ como partição especial do tópico principal.** Alguns designs colocam eventos "poison" numa partition específica do próprio tópico e um consumer alternativo lê só de lá. Rejeitada porque acopla topology de partitioning à política de erro — muda uma, quebra a outra. Tópico separado é mais claro e independente.

**2PC entre Kafka commit e Postgres insert do DLQ.** Rejeitada pela mesma razão do ADR-001 — 2PC não é maduro em Kafka e adiciona operabilidade sem ganho. Aceitamos janela mínima onde o insert no DLQ acontece mas o commit do Kafka falha, e o próximo poll re-entrega — a idempotência por event_id no DLQ absorve isso.

## Revisão

Reavaliar em qualquer um destes cenários:

- **Volume no DLQ vira ruído recorrente** (>10 eventos venenosos/dia em produção) — sinal de que retry inline não segura a barra e precisamos de retry assíncrono ou de análise de causa raiz sistemática.
- **Latência do consumer** passa a ser sensível (SLA sub-500ms end-to-end de algum consumer downstream) — 700ms de retry no pior caso pode dominar. Refinar pra retry só em exceptions transientes conhecidas.
- **Padrões de erro se estabilizam** — quando tivermos histórico real de quais exceptions são transientes vs. permanentes, refinar o retry pra ser específico por tipo (`DbUpdateConcurrencyException` retry sim; `InvalidOperationException` DLQ direto).
- **Replay volume grande** — se replays viram operação frequente (dezenas/dia), a UI atual (curl no endpoint) fica insuficiente. Dashboard SPA passa a fazer sentido, e sai do escopo v0.1.
