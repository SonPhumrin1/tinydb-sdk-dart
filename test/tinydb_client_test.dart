import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';
import 'package:tinydb_client/tinydb_client.dart';

void main() {
  const baseCollectionResponse = {
    'id': 'users',
    'tenant_id': 'tenant-1',
    'name': 'users',
    'app_id': null,
    'schema_json': '{"fields":{"name":{"type":"string","required":true}}}',
    'primary_key_field': 'uid',
    'primary_key_type': 'string',
    'primary_key_auto': false,
    'created_at': '2024-01-01T00:00:00Z',
    'updated_at': '2024-01-01T00:00:00Z',
    'deleted_at': null
  };

    group('me endpoint', () {
      test('fetches current auth profile', () async {
        http.Request? captured;
        final mock = MockClient((request) async {
          captured = request;
          expect(request.method, 'GET');
          expect(request.url.path, '/api/me');
          return http.Response(
            jsonEncode({
              'tenant_id': 'tenant-1',
              'tenant_name': 'Acme Corp',
              'app_id': 'app-7',
              'app_name': 'Console',
              'status': 'ok',
              'key_prefix': 'abc123',
              'created_at': '2024-01-01T00:00:00Z',
              'last_used': '2024-01-02T12:34:56Z',
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        });

        final client = TinyDBClient(
          endpoint: 'https://db.example.com',
          apiKey: 'demo-key',
          appId: 'app-7',
          httpClient: mock,
        );

        final profile = await client.me();

        expect(profile.tenantId, 'tenant-1');
        expect(profile.tenantName, 'Acme Corp');
        expect(profile.appId, 'app-7');
        expect(profile.appName, 'Console');
        expect(profile.status, 'ok');
        expect(profile.keyPrefix, 'abc123');
        expect(profile.createdAt, DateTime.parse('2024-01-01T00:00:00Z'));
        expect(profile.lastUsed, DateTime.parse('2024-01-02T12:34:56Z'));
        expect(captured?.headers['X-API-Key'], 'demo-key');
        expect(captured?.headers['X-App-ID'], 'app-7');

        await client.close();
      });
    });

  group('syncCollections', () {
    test('creates collection and records when missing', () async {
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode({
              'id': 'users',
              'tenant_id': 'tenant-1',
              'name': 'users',
              'app_id': null,
              'schema_json': '{}',
              'primary_key_field': 'uid',
              'primary_key_type': 'string',
              'primary_key_auto': false,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' && request.url.path == '/api/collections') {
          return http.Response('[]', 200,
              headers: {'content-type': 'application/json'});
        }
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          expect(jsonDecode(request.body), containsPair('name', 'users'));
          return http.Response(
            jsonEncode({
              'id': 'users',
              'tenant_id': 'tenant-1',
              'name': 'users',
              'app_id': null,
              'schema_json':
                  '{"fields":{"uid":{"type":"string"},"name":{"type":"string"}}}',
              'primary_key_field': 'uid',
              'primary_key_type': 'string',
              'primary_key_auto': false,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' &&
            request.url.path ==
                '/api/collections/users/documents/primary/user-1') {
          return http.Response('not found', 404);
        }
        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/documents') {
          final body = jsonDecode(request.body) as Map<String, dynamic>;
          expect(body['uid'], 'user-1');
          expect(body['name'], 'Alice');
          return http.Response(
            jsonEncode({
              'id': 'doc-1',
              'tenant_id': 'tenant-1',
              'collection_id': 'users',
              'key': 'user-1',
              'key_numeric': null,
              'data': body,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        fail('Unhandled request: ${request.method} ${request.url.path}');
      });

      final client = TinyDBClient(
        endpoint: 'https://db.example.com',
        apiKey: 'demo-key',
        httpClient: mock,
      );

      final result = await client.syncCollections([
        CollectionSyncEntry(
          name: 'users',
          schema: CollectionSchemaDefinition(fields: {
            'uid': FieldDefinition.string(required: true),
            'name': FieldDefinition.string(required: true),
          }),
          primaryKey: const PrimaryKeyConfig(
            field: 'uid',
            type: PrimaryKeyType.string,
          ),
          records: const [
            {'uid': 'user-1', 'name': 'Alice'},
          ],
        ),
      ]);

      expect(result.created, 1);
      expect(result.recordTotals.created, 1);
      expect(result.hasFailures, isFalse);
      expect(result.reports, hasLength(1));
      expect(result.reports.first.status, CollectionSyncStatus.created);
    });

    test('updates existing record when payload differs', () async {
      final collectionResponse = {
        'id': 'users',
        'tenant_id': 'tenant-1',
        'name': 'users',
        'app_id': null,
        'schema_json':
            '{"fields":{"uid":{"type":"string"},"name":{"type":"string"}}}',
        'primary_key_field': 'uid',
        'primary_key_type': 'string',
        'primary_key_auto': false,
        'created_at': '2024-01-01T00:00:00Z',
        'updated_at': '2024-01-01T00:00:00Z',
        'deleted_at': null,
      };

      var collectionsCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode({
              'id': 'users',
              'tenant_id': 'tenant-1',
              'name': 'users',
              'app_id': null,
              'schema_json': '{}',
              'primary_key_field': 'uid',
              'primary_key_type': 'string',
              'primary_key_auto': false,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' && request.url.path == '/api/collections') {
          collectionsCalls += 1;
          return http.Response(
            jsonEncode([collectionResponse]),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response('conflict', 409);
        }
        if (request.method == 'GET' &&
            request.url.path ==
                '/api/collections/users/documents/primary/user-1') {
          return http.Response(
            jsonEncode({
              'id': 'doc-1',
              'tenant_id': 'tenant-1',
              'collection_id': 'users',
              'key': 'user-1',
              'key_numeric': null,
              'data': {
                'uid': 'user-1',
                'name': 'Alice',
                'role': 'Engineer',
              },
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-02T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'PATCH' &&
            request.url.path == '/api/collections/users/documents/doc-1') {
          final body = jsonDecode(request.body) as Map<String, dynamic>;
          expect(body['role'], 'Staff Engineer');
          expect(body.containsKey('uid'), isFalse);
          return http.Response(
            jsonEncode({
              'id': 'doc-1',
              'tenant_id': 'tenant-1',
              'collection_id': 'users',
              'key': 'user-1',
              'key_numeric': null,
              'data': {
                'uid': 'user-1',
                'name': 'Alice',
                'role': 'Staff Engineer',
              },
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-02T12:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        fail('Unhandled request: ${request.method} ${request.url.path}');
      });

      final client = TinyDBClient(
        endpoint: 'https://db.example.com',
        apiKey: 'demo-key',
        httpClient: mock,
      );

      final result = await client.syncCollections([
        CollectionSyncEntry(
          name: 'users',
          records: const [
            {
              'uid': 'user-1',
              'role': 'Staff Engineer',
            }
          ],
        ),
      ]);

      expect(result.updated, 0);
      expect(result.unchanged, 1);
      expect(result.recordTotals.updated, 1);
      expect(result.hasFailures, isFalse);
      expect(collectionsCalls, greaterThanOrEqualTo(1));
    });

    test('throws when record sync fails', () async {
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode({
              'id': 'users',
              'tenant_id': 'tenant-1',
              'name': 'users',
              'app_id': null,
              'schema_json': '{}',
              'primary_key_field': 'uid',
              'primary_key_type': 'string',
              'primary_key_auto': false,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' && request.url.path == '/api/collections') {
          return http.Response('[]', 200,
              headers: {'content-type': 'application/json'});
        }
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode({
              'id': 'users',
              'tenant_id': 'tenant-1',
              'name': 'users',
              'app_id': null,
              'schema_json': '{}',
              'primary_key_field': 'uid',
              'primary_key_type': 'string',
              'primary_key_auto': false,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' &&
            request.url.path ==
                '/api/collections/users/documents/primary/user-1') {
          return http.Response('not found', 404);
        }
        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/documents') {
          return http.Response('oops', 500);
        }
        fail('Unhandled request: ${request.method} ${request.url.path}');
      });

      final client = TinyDBClient(
        endpoint: 'https://db.example.com',
        apiKey: 'demo-key',
        httpClient: mock,
      );

      expect(
        () => client.syncCollections([
          CollectionSyncEntry(
            name: 'users',
            records: const [
              {'uid': 'user-1', 'name': 'Alice'},
            ],
          ),
        ]),
        throwsA(isA<CollectionSyncException>()),
      );
    });
  });

  test('sync creates or updates collection schema', () async {
    final requests = <http.Request>[];
    final mock = MockClient((request) async {
      requests.add(request);
      if (request.method == 'POST' && request.url.path == '/api/collections') {
        final body = jsonDecode(request.body) as Map<String, dynamic>;
        expect(body['name'], 'users');
        expect(body['schema'], isNotNull);
        return http.Response(
          jsonEncode(baseCollectionResponse),
          200,
          headers: {'content-type': 'application/json'},
        );
      }
      return http.Response('not found', 404);
    });

    final client = TinyDBClient(
      endpoint: 'https://db.example.com',
      apiKey: 'demo-key',
      httpClient: mock,
    );

    final collection = await client
        .collection<JsonMap>('users')
        .schema(
          CollectionSchemaDefinition(fields: {
            'name': FieldDefinition.string(required: true),
          }),
        )
        .primaryKey(
          const PrimaryKeyConfig(field: 'uid', type: PrimaryKeyType.string),
        )
        .sync();

    expect(collection.details.name, 'users');
    expect(collection.details.primaryKeyField, 'uid');
    expect(requests, hasLength(1));
    expect(requests.first.headers['X-API-Key'], 'demo-key');
  });

  test('create document returns hydrated record', () async {
    var callIndex = 0;
    final mock = MockClient((request) async {
      callIndex += 1;
      if (callIndex == 1 &&
          request.method == 'POST' &&
          request.url.path == '/api/collections') {
        return http.Response(
          jsonEncode(baseCollectionResponse),
          200,
          headers: {'content-type': 'application/json'},
        );
      }

      if (request.method == 'POST' &&
          request.url.path == '/api/collections/users/documents') {
        final body = jsonDecode(request.body) as Map<String, dynamic>;
        expect(body['name'], 'Alice');
        return http.Response(
          jsonEncode({
            'id': 'doc-1',
            'tenant_id': 'tenant-1',
            'collection_id': 'users',
            'key': 'doc-1',
            'key_numeric': null,
            'data': {
              '_doc_id': 'doc-1',
              'name': 'Alice',
            },
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z',
            'deleted_at': null,
          }),
          200,
          headers: {'content-type': 'application/json'},
        );
      }

      return http.Response('not found', 404);
    });

    final client = TinyDBClient(
      endpoint: 'https://db.example.com',
      apiKey: 'demo-key',
      httpClient: mock,
    );

    final collection = await client.collection<JsonMap>('users').sync();

    final created = await collection.create({'name': 'Alice'});

    expect(created.id, 'doc-1');
    expect(created.data['name'], 'Alice');
    expect(created.data['_doc_id'], 'doc-1');
  });

  group('CollectionClient.syncDocuments', () {
    test('creates missing documents', () async {
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode({
              'id': 'users',
              'tenant_id': 'tenant-1',
              'name': 'users',
              'app_id': null,
              'schema_json': '{}',
              'primary_key_field': 'uid',
              'primary_key_type': 'string',
              'primary_key_auto': false,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' && request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode([
              {
                'id': 'users',
                'tenant_id': 'tenant-1',
                'name': 'users',
                'app_id': null,
                'schema_json': '{}',
                'primary_key_field': 'uid',
                'primary_key_type': 'string',
                'primary_key_auto': false,
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z',
                'deleted_at': null,
              }
            ]),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' &&
            request.url.path ==
                '/api/collections/users/documents/primary/user-1') {
          return http.Response('not found', 404);
        }
        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/documents') {
          final body = jsonDecode(request.body) as Map<String, dynamic>;
          expect(body['uid'], 'user-1');
          return http.Response(
            jsonEncode({
              'id': 'doc-1',
              'tenant_id': 'tenant-1',
              'collection_id': 'users',
              'key': 'user-1',
              'key_numeric': null,
              'data': body,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        fail('Unexpected request: ${request.method} ${request.url.path}');
      });

      final client = TinyDBClient(
        endpoint: 'https://db.example.com',
        apiKey: 'demo-key',
        httpClient: mock,
      );

      final collection = await client.collection<JsonMap>('users').sync();
      final stats = await collection.syncDocuments([
        {'uid': 'user-1', 'name': 'Alice'},
      ]);

      expect(stats.created, 1);
      expect(stats.failed, 0);
      expect(stats.unchanged, 0);
    });

    test('patches existing documents by default', () async {
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode({
              'id': 'users',
              'tenant_id': 'tenant-1',
              'name': 'users',
              'app_id': null,
              'schema_json': '{}',
              'primary_key_field': 'uid',
              'primary_key_type': 'string',
              'primary_key_auto': false,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' && request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode([
              {
                'id': 'users',
                'tenant_id': 'tenant-1',
                'name': 'users',
                'app_id': null,
                'schema_json': '{}',
                'primary_key_field': 'uid',
                'primary_key_type': 'string',
                'primary_key_auto': false,
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z',
                'deleted_at': null,
              }
            ]),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' &&
            request.url.path ==
                '/api/collections/users/documents/primary/user-1') {
          return http.Response(
            jsonEncode({
              'id': 'doc-1',
              'tenant_id': 'tenant-1',
              'collection_id': 'users',
              'key': 'user-1',
              'key_numeric': null,
              'data': {
                'uid': 'user-1',
                'name': 'Alice',
                'role': 'Engineer',
              },
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'PATCH' &&
            request.url.path == '/api/collections/users/documents/doc-1') {
          final body = jsonDecode(request.body) as Map<String, dynamic>;
          expect(body.containsKey('uid'), isFalse);
          expect(body['role'], 'Staff Engineer');
          return http.Response(
            jsonEncode({
              'id': 'doc-1',
              'tenant_id': 'tenant-1',
              'collection_id': 'users',
              'key': 'user-1',
              'key_numeric': null,
              'data': {
                'uid': 'user-1',
                'name': 'Alice',
                'role': 'Staff Engineer',
              },
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-02T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        fail('Unexpected request: ${request.method} ${request.url.path}');
      });

      final client = TinyDBClient(
        endpoint: 'https://db.example.com',
        apiKey: 'demo-key',
        httpClient: mock,
      );
      final collection = await client.collection<JsonMap>('users').sync();

      final stats = await collection.syncDocuments([
        {'uid': 'user-1', 'role': 'Staff Engineer'},
      ]);

      expect(stats.updated, 1);
      expect(stats.unchanged, 0);
    });

    test('throws RecordSyncException when sync fails', () async {
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode({
              'id': 'users',
              'tenant_id': 'tenant-1',
              'name': 'users',
              'app_id': null,
              'schema_json': '{}',
              'primary_key_field': 'uid',
              'primary_key_type': 'string',
              'primary_key_auto': false,
              'created_at': '2024-01-01T00:00:00Z',
              'updated_at': '2024-01-01T00:00:00Z',
              'deleted_at': null,
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' && request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode([
              {
                'id': 'users',
                'tenant_id': 'tenant-1',
                'name': 'users',
                'app_id': null,
                'schema_json': '{}',
                'primary_key_field': 'uid',
                'primary_key_type': 'string',
                'primary_key_auto': false,
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z',
                'deleted_at': null,
              }
            ]),
            200,
            headers: {'content-type': 'application/json'},
          );
        }
        if (request.method == 'GET' &&
            request.url.path ==
                '/api/collections/users/documents/primary/user-1') {
          return http.Response('not found', 404);
        }
        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/documents') {
          return http.Response('oops', 500);
        }
        fail('Unexpected request: ${request.method} ${request.url.path}');
      });

      final client = TinyDBClient(
        endpoint: 'https://db.example.com',
        apiKey: 'demo-key',
        httpClient: mock,
      );
      final collection = await client.collection<JsonMap>('users').sync();

      expect(
        () => collection.syncDocuments([
          {'uid': 'user-1', 'name': 'Alice'},
        ]),
        throwsA(isA<RecordSyncException>()),
      );
    });
  });
}
