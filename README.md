# Infra utilities

This repository provides the `x.encore.dev/infra` module, comprising utility packages
for effectively working with cloud infrastructure using [Encore](https://github.com/encoredev/encore).

### Transactional Outbox

[![PkgGoDev](https://pkg.go.dev/badge/x.encore.dev/infra/pubsub/outbox)](https://pkg.go.dev/x.encore.dev/infra/pubsub/outbox)

The `pubsub/outbox` package provides a transactional outbox for transactional publishing of
messages to a pubsub topic. This is useful when you want to publish a message as part of a
database transaction and tie the publishing to the fate (commit/rollback) of the transaction.

Use the "Go reference" link above for more information.
