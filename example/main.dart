import 'dart:convert';
import 'dart:io';

import 'package:tinydb_client/tinydb_client.dart';

Future<void> main() async {
  final endpoint =
      Platform.environment['TINYDB_ENDPOINT'] ?? 'http://localhost:8080';
  final apiKey = "584ad90972329e8d8ecabc5f3f18471746ed680dc148c307b926a75fade5ae07"; //Platform.environment['TINYDB_API_KEY'];
  if (apiKey == null || apiKey.isEmpty) {
    stderr.writeln('Missing TINYDB_API_KEY environment variable.');
    stderr.writeln(
        'Export TINYDB_API_KEY (and optionally TINYDB_ENDPOINT, TINYDB_APP_ID, TINYDB_COLLECTION) before running.');
    exitCode = 64; // EX_USAGE
    return;
  }

  final appIdRaw = Platform.environment['TINYDB_APP_ID'];
  final appId = (appIdRaw == null || appIdRaw.isEmpty) ? null : appIdRaw;
  final collectionName =
      Platform.environment['TINYDB_COLLECTION'] ?? 'dart_demo_users';

  stdout.writeln('Connecting to $endpoint');
  stdout.writeln('Using collection: $collectionName');

  final client = TinyDBClient(
    endpoint: endpoint,
    apiKey: apiKey,
    appId: appId,
  );

  try {
    final collection = await client
        .collection<JsonMap>(collectionName)
        .schema(
          CollectionSchemaDefinition(fields: {
            'uid': FieldDefinition.string(required: true),
            'name': FieldDefinition.string(required: true),
            'email': FieldDefinition.string(),
            'role': FieldDefinition.string(),
            'age': FieldDefinition.number(),
          }),
        )
        .primaryKey(
          const PrimaryKeyConfig(field: 'uid', type: PrimaryKeyType.string),
        )
        .sync();

    stdout.writeln(
      'Collection ready (id=${collection.details.id}, primaryKey=${collection.details.primaryKeyField})',
    );

    final uid = 'dart-${DateTime.now().microsecondsSinceEpoch}';
    final created = await collection.create({
      'uid': uid,
      'name': 'Dart SDK Tester',
      'email': 'tester+$uid@example.com',
      'role': 'QA Engineer',
      'age': 29,
    });
    stdout.writeln('Created document: ${jsonEncode(created.data)}');

    final fetched = await collection.get(created.id);
    stdout.writeln('Fetched document: ${jsonEncode(fetched.data)}');

    final patched = await collection.patch(created.id, {
      'role': 'Staff Engineer',
      'age': 30,
    });
    stdout.writeln('Patched document: ${jsonEncode(patched.data)}');

    final listed = await collection.list();
    stdout.writeln(
        'Collection now has ${listed.items.length} document(s) in the first page.');

    final query = await collection.query({
      'where': {
        'and': [
          {
            'uid': {
              'eq': uid,
            },
          },
        ],
      },
      'limit': 1,
    });
    stdout.writeln(
        'Query returned ${query.items.length} document(s) for uid=$uid');

    final syncResult = await collection.sync();
    stdout.writeln(
        'Sync returned ${syncResult.items.length} change(s) (pagination count: ${syncResult.pagination.count ?? 0}).');

    await collection.delete(created.id);
    stdout.writeln('Deleted document ${created.id}.');
  } on TinyDBException catch (error) {
    stderr.writeln('TinyDB error (${error.status}): ${error.message}');
    if (error.details != null) {
      stderr.writeln(jsonEncode(error.details));
    }
    exitCode = error.status;
  } catch (error, stackTrace) {
    stderr.writeln('Unexpected error: $error');
    stderr.writeln(stackTrace);
    exitCode = 1;
  } finally {
    await client.close();
  }
}
