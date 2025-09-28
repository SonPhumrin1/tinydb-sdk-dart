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
    endpoint: 'https://api.tinydb.com',
    apiKey: 'your-api-key',
    appId: 'optional-app-id',
  );

  final users = await db.collection('users').schema(
    CollectionSchemaDefinition(fields: {
      'name': FieldDefinition.string(required: true),
      'email': FieldDefinition.string(),
      'age': FieldDefinition.number(),
    }),
  ).primaryKey(
    PrimaryKeyConfig(field: 'uid', type: PrimaryKeyType.uuid, auto: true),
  ).sync();

  final created = await users.create({
    'name': 'Alice',
    'email': 'alice@example.com',
    'age': 31,
  });

  final fetched = await users.get(created.id);
  print(fetched.data);
}
```

See `example/main.dart` for a more complete walkthrough.

## License

MIT © CUBIS Labs
