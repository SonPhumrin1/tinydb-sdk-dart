import 'dart:convert';
import 'dart:io';

import 'package:tinydb_client/tinydb_client.dart';

Future<void> main() async {
  final endpoint =
      Platform.environment['TINYDB_ENDPOINT'] ?? 'http://localhost:8080';
  final envApiKey = Platform.environment['TINYDB_API_KEY'];
  if (envApiKey == null || envApiKey.isEmpty) {
    stderr.writeln('Missing TINYDB_API_KEY environment variable.');
    stderr.writeln(
        'Export TINYDB_API_KEY (and optionally TINYDB_ENDPOINT, TINYDB_APP_ID, TINYDB_COLLECTION) before running.');
    exitCode = 64; // EX_USAGE
    return;
  }
  final apiKey = envApiKey;

  final appIdRaw = Platform.environment['TINYDB_APP_ID'];
  final appId = (appIdRaw == null || appIdRaw.isEmpty) ? null : appIdRaw;
  final collectionName = 'dart_demo_users';

  stdout.writeln('Connecting to $endpoint');
  stdout.writeln('Using collection: $collectionName');

  final client = TinyDBClient(
    endpoint: endpoint,
    apiKey: apiKey,
    appId: appId,
  );

  try {
    final uid = 'dart-${DateTime.now().microsecondsSinceEpoch}';
    final syncResult = await client.syncCollections([
      CollectionSyncEntry(
        name: collectionName,
        schema: CollectionSchemaDefinition(fields: {
          'uid': FieldDefinition.string(required: true),
          'name': FieldDefinition.string(required: true),
          'email': FieldDefinition.string(),
          'role': FieldDefinition.string(),
          'age': FieldDefinition.number(),
          'created_at': FieldDefinition.datetime(),
        }),
        primaryKey:
            const PrimaryKeyConfig(field: 'uid', type: PrimaryKeyType.string),
        records: [
          {
            'uid': uid,
            'name': 'Dart SDK Tester',
            'email': 'tester+$uid@example.com',
            'role': 'QA Engineer',
            'age': 29,
            'created_at': DateTime.now().toUtc().toIso8601String(),
          },
          {
            'uid': 'dart-2',
            'name': 'Sambo Chea',
            'email': 'sambo.chea@example.com',
            'role': 'Software Engineer',
            'age': 39,
            'created_at': DateTime.now().toUtc().toIso8601String(),
          }
        ],
      ),
    ]);

    for (final report in syncResult.reports) {
      stdout.writeln(
          'Synced collection ${report.name} -> ${report.status.name} (records: +${report.recordStats.created}/~${report.recordStats.updated})');
      if (report.error != null) {
        stdout.writeln('  warning: ${report.error}');
      }
    }

    final collection = await client.collection<JsonMap>(collectionName).sync();
    stdout.writeln(
      'Collection ready (id=${collection.details.id}, primaryKey=${collection.details.primaryKeyField})',
    );

    final extraUid = '${uid}_companion';
    final docStats = await collection.syncDocuments([
      {
        'uid': uid,
        'role': 'Staff Engineer',
        'age': 30,
        'skills': ['Dart', 'Go'],
      },
      {
        'uid': extraUid,
        'name': 'Dart SDK Companion',
        'role': 'Developer Advocate',
      },
    ]);
    stdout.writeln(
        'Document sync stats: created=${docStats.created} updated=${docStats.updated} skipped=${docStats.skipped} failed=${docStats.failed}');

    final fetched = await collection.getByPrimaryKey(uid);
    stdout.writeln('Fetched document: ${jsonEncode(fetched.data)}');
    final companion = await collection.getByPrimaryKey(extraUid);
    stdout.writeln('Companion document: ${jsonEncode(companion.data)}');

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

    final syncChanges = await collection.sync();
    stdout.writeln(
        'Sync returned ${syncChanges.items.length} change(s) (pagination count: ${syncChanges.pagination.count ?? 0}).');

    await collection.delete([fetched.id, companion.id]);
    stdout.writeln('Deleted documents ${fetched.id} and ${companion.id}.');
  } on CollectionSyncException catch (error) {
    stderr.writeln('Collection sync failed: ${error.message}');
    for (final report in error.result.reports) {
      stderr.writeln(
          '  ${report.name}: ${report.status} (records failed=${report.recordStats.failed})');
      if (report.error != null) {
        stderr.writeln('    ${report.error}');
      }
    }
    exitCode = 1;
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
