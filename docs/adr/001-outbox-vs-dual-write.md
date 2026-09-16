# ADR-001 — Outbox transacional vs. dual write

**Status:** accepted
**Data:** 2026-09-16
**Autor:** Zeca

## Contexto

O `event-spine` precisa garantir que **toda mudança de estado persistida no Postgres é publicada como evento no Redpanda, exatamente uma vez do ponto de vista do consumer** (na prática: pelo menos uma vez, com idempotência downstream). A pergunta é *como* fazer essa dupla escrita — banco e broker — sem abrir uma janela onde uma acontece e a outra não.

O problema clássico:

- Se escrevermos primeiro no banco e depois no broker, um crash entre as duas operações deixa o estado no banco sem evento correspondente. Consumidores nunca saberão.
- Se escrevermos primeiro no broker e depois no banco, um crash inverso publica um evento que não corresponde a estado nenhum. Consumidores reagem a algo que não existe.
- Se envolvermos os dois numa transação distribuída (XA/2PC), pagamos coordenação cara em produção, dependemos de suporte do broker (Kafka/Redpanda não têm XA nativo bem estabelecido) e engessamos evolução.

A entrega principal do `event-spine` é uma implementação de referência dos padrões que arquitetos sêniores são cobrados em entrevista e em revisão de design. Escolher errado aqui compromete o resto do projeto.

## Decisão

**Outbox transacional em Postgres, com relay assíncrono para Redpanda.**

O producer, dentro da mesma transação que atualiza a tabela de domínio (`orders`), insere uma linha correspondente na tabela `outbox` com o payload do evento serializado. Commit atômico — ou os dois inserts persistem juntos, ou nenhum.

Um worker separado (`OutboxRelay`, `BackgroundService` no mesmo processo do producer ou em processo apartado — decisão em runtime) faz *poll* periódico das linhas `sent_at IS NULL`, publica cada uma no tópico apropriado do Redpanda usando `key = <aggregate_id>` (garante ordem por agregado), e marca `sent_at = now()` depois do ack do broker.

Esse desenho:

- **Garante consistência** entre banco e eventos: se `orders` tem uma linha, o outbox tem a linha correspondente.
- **Custa uma tabela e um worker**, sem introduzir 2PC nem coordenação distribuída.
- **É reentrante**: se o relay crasha entre publicar e marcar sent, o próximo poll reenvia. O consumer descarta via idempotência (ver ADR-003, pendente).
- **É debugável**: o outbox é uma tabela SQL comum — dá pra inspecionar, contar backlog, entender atrasos com queries triviais.

Ordem por agregado é preservada usando `key = order_id` na produção — Redpanda entrega mensagens da mesma chave sempre à mesma partition, na ordem em que foram produzidas.

## Consequências

**Positivas**

- Consistência forte entre estado e eventos, sem 2PC.
- Rastreabilidade: `SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL` é o backlog observável do sistema.
- Independência do broker: mesma técnica funciona com Kafka, Redpanda, RabbitMQ com pequenos ajustes.
- Fácil de testar: outbox é SQL, testcontainers com Postgres cobrem 80% dos cenários sem precisar de broker real.

**Negativas**

- **Latência mínima aumenta** — o evento não é publicado no mesmo instante da transação, mas no próximo ciclo do relay. Para o polling configurado em 200 ms, latência esperada é `50–200 ms` acima do commit. Aceitável para o caso de referência; sistemas com SLA sub-100 ms end-to-end precisam avaliar.
- **Backlog no outbox** vira problema operacional próprio: se o broker cai por 1 h, o outbox acumula 1 h de eventos. O relay reprocessa quando volta, mas queries no Postgres pesam durante o catch-up. Mitigação: index em `(sent_at, id)`, batch size na leitura, alertas de backlog.
- **Order-by-key ≠ order-by-time-global** — dois `orders` diferentes podem ser vistos pelos consumers em ordem diferente da ordem em que foram inseridos. Isso é intencional (paraleliza melhor), mas requer que consumers não assumam ordem global. Documentado no README dos consumers.
- **Escalabilidade do relay** — um único worker é o gargalo natural. Para v0.1 aceitamos isso; se virar problema, particionamento por hash de aggregate_id divide entre N workers.

**Neutras**

- Schema do payload no outbox é opaco (bytes / json). Evolução de schema é decisão separada, coberta por schema registry na v0.1 final.

## Alternativas rejeitadas

**Dual write "otimista" (banco primeiro, broker depois, log de erro se broker falhar).** Rejeitada porque perde eventos silenciosamente em cenários de crash do processo entre as duas operações — precisamente a classe de bug que outbox existe para eliminar. Nenhum log ou alerta consegue reconstruir o evento perdido.

**2PC (XA transactions).** Rejeitada porque Postgres suporta XA mas Redpanda/Kafka não tem participante XA maduro; forçaria um coordenador de transações externo (Narayana, Atomikos), o que aumenta operabilidade e reduz throughput em uma ou duas ordens de grandeza. Além disso, XA é notoriamente frágil a partições de rede.

**CDC (Debezium lendo o WAL do Postgres).** Rejeitada para v0.1 por adicionar um componente pesado (Kafka Connect + Debezium) que rouba o palco do padrão que queremos ilustrar. Interessante para v0.2+, especialmente para sistemas legacy que já têm estado no banco e não podem mudar o código do producer — nesse caso, CDC lê o WAL e produz eventos sem tocar no producer. Fica anotado como alternativa reconhecida.

**Event sourcing puro (banco = event store, projeções derivadas).** Rejeitada porque muda o modelo de escrita do sistema inteiro: não faz sentido para o objetivo desta implementação de referência, que é ensinar o padrão *outbox* especificamente. Um projeto de referência de event sourcing puro seria outro repositório.

## Revisão

Reavaliar em qualquer um destes cenários:

- **Backlog do outbox** vira problema operacional recorrente (mais de 3 alertas em 30 dias).
- Apareceu **SLA sub-100 ms** end-to-end como requisito de algum consumer sobre um evento.
- Um consumer real precisa de **ordem global** entre agregados diferentes (caso raro, mas se surgir força repensar a chave de partição).
- **Debezium** virar dependência em outro projeto do portfólio, tornando natural integrar aqui como opção paralela em v0.2.
