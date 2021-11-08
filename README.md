# Eventlog Example

This example is showcasing an [eventually consistent](https://en.wikipedia.org/wiki/Data_integrity), fault-tolerant, [event sourced](https://de.wikipedia.org/wiki/Event_Sourcing) system following the [CQRS (Command-Query-Responsibility-Segregation)](https://en.wikipedia.org/wiki/Command–query_separation#Command_query_responsibility_segregation) principle consisting of two individual [microservices](https://en.wikipedia.org/wiki/Microservices) using [romshark/eventlog](github.com/romshark/eventlog) as both an event database and communication bus. The producer service produces events of objects being put into a virtual pile, while the consumer service consumes them and projects the current state of the pile onto its [dgraph-io/badger](https://github.com/dgraph-io/badger) key-value database. The producer service also projects the world onto its own database to transactionally check for invariants (preventing `take` events from being commited when there aren't enough objects in the pile). The system uses [OCC (Optimistic Concurrency Control)](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) to enforce invariants and keep the log in a consistent and valid state.

## Getting started

- Run the eventlog in in-memory mode: `eventlog inmem -http-host :9090`
- Run the consumer: `cd cmd/consumer && go run main.go -log-addr :9090`
- Run the producer: `cd cmd/producer && go run main.go -log-addr :9090`
- Optionally, you can use `-db-dir` on both the consumer and producer to make them use an actual persistent database, otherwise they will use an in-memory database by default. `-db-log` will enable more detailed database debug logs.

The order in which the services are run isn't important, the system will automatically try to (re)connect to the log indefinitely.