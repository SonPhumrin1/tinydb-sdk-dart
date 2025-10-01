# TinyDB Dart Client SDK

Dart/Flutter SDK for interacting with TinyDB. Supports collection management, schema updates, document CRUD, bulk operations, and incremental sync.

## Installation

Add the local package to your project (until published to pub.dev):

```yaml
dependencies:
    tinydb_client:
        git:
            url: https://github.com/cubetiqlabs/tinydb-sdk-dart.git
            ref: main
```

Then run `dart pub get`.

## Quick Start

```dart
import 'package:tinydb_client/tinydb_client.dart';

Future<void> main() async {
  final db = TinyDBClient(
    endpoint: 'https://api.tinydb.com', // or Platform.environment['TINYDB_ENDPOINT']
    apiKey: 'your-api-key',
    appId: 'optional-app-id',
  );

  final users = await db
      .collection('users')
      .schema(
        CollectionSchemaDefinition(fields: {
          'uid': FieldDefinition.string(required: true),
          'name': FieldDefinition.string(required: true),
          'email': FieldDefinition.string(),
          'age': FieldDefinition.number(),
        }),
      )
      .primaryKey(
        const PrimaryKeyConfig(field: 'uid', type: PrimaryKeyType.string),
      )
      .sync();

  final created = await users.create({
    'uid': 'user-${DateTime.now().millisecondsSinceEpoch}',
    'name': 'Alice',
    'email': 'alice@example.com',
    'age': 31,
  });

  final fetched = await users.get(created.id);
  print('Document version: ${fetched.version}');
  print('Fetched user: ${fetched.data}');
}
```

See `example/main.dart` for a more complete walkthrough.

### Sync collections and documents together

```dart
final result = await db.syncCollections([
  CollectionSyncEntry(
    name: 'users',
    schema: CollectionSchemaDefinition(fields: {
      'uid': FieldDefinition.string(required: true),
      'name': FieldDefinition.string(required: true),
      'email': FieldDefinition.string(),
    }),
    primaryKey: const PrimaryKeyConfig(
      field: 'uid',
      type: PrimaryKeyType.string,
    ),
    records: const [
      {'uid': 'user-1', 'name': 'Alice', 'email': 'alice@example.com'},
      {'uid': 'user-2', 'name': 'Bob'},
    ],
  ),
]);

for (final report in result.reports) {
  print('${report.name}: ${report.status.name} (records created=${report.recordStats.created})');
}
```

Provide one or more `CollectionSyncEntry` items. Each entry provisions the collection schema (creating or updating as needed) and synchronizes the `records` array using the collection's primary key. By default documents are patched (`RecordSyncMode.patch`); pass `recordsMode: RecordSyncMode.update` to perform full replacements.

### Sync documents on an existing collection

```dart
final users = await db.collection<JsonMap>('users').sync();
final stats = await users.syncDocuments([
  {'uid': 'user-1', 'name': 'Alice'},
  {'uid': 'user-2', 'name': 'Bob', 'role': 'DevOps'},
]);

if (stats.failed > 0) {
  throw RecordSyncException('document sync failed', stats);
}

print('Created ${stats.created}, updated ${stats.updated}, skipped ${stats.skipped} records');
```

`syncDocuments` mirrors the CLI's record import behaviour: it looks up each document by primary key, creates it if missing, otherwise patches (or updates) the mutable fields while preserving data integrity.

### Iterate cursor-based query results

```dart
final users = await db.collection<JsonMap>('users').sync();
final aggregated = await users.queryAll(
  {
    'where': {
      'and': [
        {
          'status': {'eq': 'active'},
        },
      ],
    },
  },
  pageLimit: 100,
);

print('Fetched ${aggregated.items.length} active users');
if (!aggregated.exhausted) {
  print('Resume later with cursor: ${aggregated.nextCursor}');
}
```

`queryAll` automatically follows TinyDB's cursor-based pagination. It keeps calling `/query` while `next_cursor` is present, aggregates the results, and exposes control knobs:

- `pageLimit` overrides the request limit per page.
- `maxPages` stops after a fixed number of pages (returns `exhausted = false` if more data remains).
- `maxItems` trims once the desired number of records is collected.
- `onPage` lets you inspect each `QueryResult` as it arrives (for streaming or logging).
- `onProgress` surfaces a lightweight `QueryProgress` snapshot (page count, item count, cursor, done flag) after each page is processed.

### Stream query pages lazily

```dart
await for (final page in users.queryPages(
  {
    'where': {
      'and': [
        {
          'role': {'contains': 'Engineer'},
        },
      ],
    },
  },
  pageLimit: 50,
  maxPages: 5,
)) {
  print('Processed ${page.items.length} engineer(s)');
}
```

`queryPages` returns an async stream of `QueryResult` objects, following each cursor until exhaustion (or until `maxPages`/`maxItems` thresholds are met). This is handy for chunked ETL pipelines or incremental processing without buffering the entire dataset in memory.
Pass `onProgress` to observe a rolling `QueryProgress` summary after every emitted page.

### Iterate documents as they arrive

```dart
await for (final record in users.queryStream(
  {
    'where': {
      'and': [
        {
          'role': {'contains': 'Engineer'},
        },
      ],
    },
  },
  maxItems: 250,
)) {
  print('Processing engineer: ${record.data['name']}');
}
```

`queryStream` is built on top of `queryPages` and emits each `DocumentRecord` immediately. It honours the same throttling parameters (`pageLimit`, `maxPages`, `maxItems`) so you can cap the work while still benefiting from a simple async `for` loop.
Specify `onProgress` to receive updates once each page is drained (including total items streamed so far and whether more data remains).

### Run the example against a real API

```bash
export TINYDB_ENDPOINT="https://your-tinydb-host"
export TINYDB_API_KEY="your-api-key"
# export TINYDB_APP_ID="optional-app-id"

dart run example/main.dart
```

The script provisions the collection if needed, performs CRUD operations, runs a query, and cleans up the created document.

## License

MIT © CUBIS Labs
