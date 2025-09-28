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
  print('Fetched user: ${fetched.data}');
}
```

See `example/main.dart` for a more complete walkthrough.

### Run the example against a real API

```bash
export TINYDB_ENDPOINT="https://your-tinydb-host"
export TINYDB_API_KEY="your-api-key"
# export TINYDB_APP_ID="optional-app-id"
# export TINYDB_COLLECTION="optional-collection-name"

dart run example/main.dart
```

The script provisions the collection if needed, performs CRUD operations, runs a query, and cleans up the created document.

## License

MIT © CUBIS Labs
