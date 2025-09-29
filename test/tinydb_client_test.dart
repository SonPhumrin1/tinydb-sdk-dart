import 'dart:async';
import 'dart:convert';

import 'package:async/async.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';
import 'package:tinydb_client/tinydb_client.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

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

  group('CollectionClient.queryAll', () {
    test('aggregates across cursor pages until exhaustion', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final body = jsonDecode(request.body) as Map<String, dynamic>;
          if (queryCalls == 1) {
            expect(body['cursor'], isNull);
            expect(body['limit'], 1);
            return http.Response(
              jsonEncode({
                'items': [
                  {
                    'id': 'doc-1',
                    'tenant_id': 'tenant-1',
                    'collection_id': 'users',
                    'key': 'user-1',
                    'key_numeric': null,
                    'data': {'uid': 'user-1'},
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-01T00:00:00Z',
                    'deleted_at': null,
                  },
                ],
                'pagination': {
                  'limit': 1,
                  'count': 1,
                  'next_cursor': 'cursor-2',
                },
              }),
              200,
              headers: {'content-type': 'application/json'},
            );
          }

          expect(body['cursor'], 'cursor-2');
          expect(body['limit'], 1);
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-2',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-2',
                  'key_numeric': null,
                  'data': {'uid': 'user-2'},
                  'created_at': '2024-01-01T00:00:01Z',
                  'updated_at': '2024-01-01T00:00:01Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': null,
              },
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
      final pages = <int>[];
      final aggregated = await collection.queryAll(
        {'where': {}},
        pageLimit: 1,
        onPage: (page) => pages.add(page.items.length),
      );

      expect(queryCalls, 2);
      expect(aggregated.items, hasLength(2));
      expect(aggregated.items.map((e) => e.data['uid']), ['user-1', 'user-2']);
      expect(aggregated.exhausted, isTrue);
      expect(aggregated.pageCount, 2);
      expect(pages, [1, 1]);
    });

    test('emits progress updates for each page', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final cursor = queryCalls == 1 ? 'cursor-2' : null;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-$queryCalls',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-$queryCalls',
                  'key_numeric': null,
                  'data': {'uid': 'user-$queryCalls'},
                  'created_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'updated_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': cursor,
              },
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
      final observed = <QueryProgress>[];
      final aggregated = await collection.queryAll(
        {'where': {}},
        pageLimit: 1,
        onProgress: observed.add,
      );

      expect(queryCalls, 2);
      expect(aggregated.items, hasLength(2));
      expect(observed, hasLength(2));
      expect(observed.first.pageCount, 1);
      expect(observed.first.itemCount, 1);
      expect(observed.first.done, isFalse);
      expect(observed.first.hasNextCursor, isTrue);
      expect(observed.last.pageCount, 2);
      expect(observed.last.itemCount, 2);
      expect(observed.last.done, isTrue);
      expect(observed.last.hasNextCursor, isFalse);
    });

    test('respects maxPages and exposes next cursor', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final body = jsonDecode(request.body) as Map<String, dynamic>;
          if (queryCalls == 1) {
            return http.Response(
              jsonEncode({
                'items': [
                  {
                    'id': 'doc-1',
                    'tenant_id': 'tenant-1',
                    'collection_id': 'users',
                    'key': 'user-1',
                    'key_numeric': null,
                    'data': {'uid': 'user-1'},
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-01T00:00:00Z',
                    'deleted_at': null,
                  },
                ],
                'pagination': {
                  'limit': 2,
                  'count': 1,
                  'next_cursor': 'cursor-2',
                },
              }),
              200,
              headers: {'content-type': 'application/json'},
            );
          }

          expect(body['cursor'], 'cursor-2');
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-2',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-2',
                  'key_numeric': null,
                  'data': {'uid': 'user-2'},
                  'created_at': '2024-01-01T00:00:01Z',
                  'updated_at': '2024-01-01T00:00:01Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 2,
                'count': 1,
                'next_cursor': 'cursor-3',
              },
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
      final aggregated = await collection.queryAll(
        {'where': {}},
        maxPages: 1,
      );

      expect(queryCalls, 1);
      expect(aggregated.items, hasLength(1));
      expect(aggregated.exhausted, isFalse);
      expect(aggregated.nextCursor, 'cursor-2');
    });

    test('respects maxItems and trims extras', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final cursor = queryCalls == 1 ? 'cursor-2' : null;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-$queryCalls',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-$queryCalls',
                  'key_numeric': null,
                  'data': {'uid': 'user-$queryCalls'},
                  'created_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'updated_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': cursor,
              },
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
      final aggregated = await collection.queryAll(
        {'where': {}},
        maxItems: 1,
      );

      expect(queryCalls, 1);
      expect(aggregated.items, hasLength(1));
      expect(aggregated.exhausted, isFalse);
      expect(aggregated.nextCursor, 'cursor-2');
    });

    test('stops pagination when cancellation token fires', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final cursor = queryCalls == 1 ? 'cursor-2' : null;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-$queryCalls',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-$queryCalls',
                  'key_numeric': null,
                  'data': {'uid': 'user-$queryCalls'},
                  'created_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'updated_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': cursor,
              },
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
      final source = CancellationTokenSource();
      final progress = <QueryProgress>[];

      final aggregated = await collection.queryAll(
        {'where': {}},
        pageLimit: 1,
        onProgress: (snapshot) {
          progress.add(snapshot);
          if (snapshot.pageCount >= 1) {
            source.cancel();
          }
        },
        cancellationToken: source.token,
      );

      expect(queryCalls, 1);
      expect(aggregated.pageCount, 1);
      expect(aggregated.items, hasLength(1));
      expect(aggregated.exhausted, isFalse);
      expect(progress, isNotEmpty);
      expect(progress.last.done, isTrue);
    });
  });

  group('CollectionClient.queryPages', () {
    test('streams each page until cursor exhausted', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final body = jsonDecode(request.body) as Map<String, dynamic>;
          if (queryCalls == 1) {
            expect(body['cursor'], isNull);
            expect(body['limit'], 1);
            return http.Response(
              jsonEncode({
                'items': [
                  {
                    'id': 'doc-1',
                    'tenant_id': 'tenant-1',
                    'collection_id': 'users',
                    'key': 'user-1',
                    'key_numeric': null,
                    'data': {'uid': 'user-1'},
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-01T00:00:00Z',
                    'deleted_at': null,
                  },
                ],
                'pagination': {
                  'limit': 1,
                  'count': 1,
                  'next_cursor': 'cursor-2',
                },
              }),
              200,
              headers: {'content-type': 'application/json'},
            );
          }

          expect(body['cursor'], 'cursor-2');
          expect(body['limit'], 1);
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-2',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-2',
                  'key_numeric': null,
                  'data': {'uid': 'user-2'},
                  'created_at': '2024-01-01T00:00:01Z',
                  'updated_at': '2024-01-01T00:00:01Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': null,
              },
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
      final pages =
          await collection.queryPages({'where': {}}, pageLimit: 1).toList();

      expect(queryCalls, 2);
      expect(pages, hasLength(2));
      expect(pages.first.items.single.data['uid'], 'user-1');
      expect(pages.last.items.single.data['uid'], 'user-2');
    });

    test('reports progress for each streamed page', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final cursor = queryCalls == 1 ? 'cursor-2' : null;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-$queryCalls',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-$queryCalls',
                  'key_numeric': null,
                  'data': {'uid': 'user-$queryCalls'},
                  'created_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'updated_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': cursor,
              },
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
      final observed = <QueryProgress>[];
      await collection.queryPages(
        {'where': {}},
        pageLimit: 1,
        onProgress: observed.add,
      ).drain<void>();

      expect(queryCalls, 2);
      expect(observed, hasLength(2));
      expect(observed.first.pageCount, 1);
      expect(observed.first.itemCount, 1);
      expect(observed.first.done, isFalse);
      expect(observed.last.pageCount, 2);
      expect(observed.last.itemCount, 2);
      expect(observed.last.done, isTrue);
    });

    test('honours maxPages and stops early', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final body = jsonDecode(request.body) as Map<String, dynamic>;
          if (queryCalls == 1) {
            expect(body['cursor'], isNull);
            return http.Response(
              jsonEncode({
                'items': [
                  {
                    'id': 'doc-1',
                    'tenant_id': 'tenant-1',
                    'collection_id': 'users',
                    'key': 'user-1',
                    'key_numeric': null,
                    'data': {'uid': 'user-1'},
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-01T00:00:00Z',
                    'deleted_at': null,
                  },
                ],
                'pagination': {
                  'limit': 2,
                  'count': 1,
                  'next_cursor': 'cursor-2',
                },
              }),
              200,
              headers: {'content-type': 'application/json'},
            );
          }

          expect(body['cursor'], 'cursor-2');
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-2',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-2',
                  'key_numeric': null,
                  'data': {'uid': 'user-2'},
                  'created_at': '2024-01-01T00:00:01Z',
                  'updated_at': '2024-01-01T00:00:01Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 2,
                'count': 1,
                'next_cursor': 'cursor-3',
              },
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
      final pages = await collection
          .queryPages({'where': {}}, pageLimit: 1, maxPages: 1).toList();

      expect(queryCalls, 1);
      expect(pages, hasLength(1));
      expect(pages.single.pagination.nextCursor, 'cursor-2');
    });

    test('respects maxItems and trims emitted results', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final cursor = queryCalls == 1 ? 'cursor-2' : null;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-$queryCalls',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-$queryCalls',
                  'key_numeric': null,
                  'data': {'uid': 'user-$queryCalls'},
                  'created_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'updated_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': cursor,
              },
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
      final pages =
          await collection.queryPages({'where': {}}, maxItems: 1).toList();

      expect(queryCalls, 1);
      expect(pages, hasLength(1));
      expect(pages.first.items, hasLength(1));
      expect(pages.first.pagination.nextCursor, 'cursor-2');
    });

    test('emits pages until cancellation token cancels', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final cursor = queryCalls == 1 ? 'cursor-2' : null;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-$queryCalls',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-$queryCalls',
                  'key_numeric': null,
                  'data': {'uid': 'user-$queryCalls'},
                  'created_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'updated_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': cursor,
              },
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
      final source = CancellationTokenSource();
      final progress = <QueryProgress>[];

      final pages = await collection.queryPages(
        {'where': {}},
        pageLimit: 1,
        onProgress: (snapshot) {
          progress.add(snapshot);
          source.cancel();
        },
        cancellationToken: source.token,
      ).toList();

      expect(queryCalls, 1);
      expect(pages, hasLength(1));
      expect(progress, isNotEmpty);
      expect(progress.last.done, isTrue);
    });
  });

  group('CollectionClient.queryStream', () {
    test('streams documents across cursor pages', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final body = jsonDecode(request.body) as Map<String, dynamic>;
          if (queryCalls == 1) {
            expect(body['cursor'], isNull);
            return http.Response(
              jsonEncode({
                'items': [
                  {
                    'id': 'doc-1',
                    'tenant_id': 'tenant-1',
                    'collection_id': 'users',
                    'key': 'user-1',
                    'key_numeric': null,
                    'data': {'uid': 'user-1'},
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-01T00:00:00Z',
                    'deleted_at': null,
                  },
                ],
                'pagination': {
                  'limit': 1,
                  'count': 1,
                  'next_cursor': 'cursor-2',
                },
              }),
              200,
              headers: {'content-type': 'application/json'},
            );
          }

          expect(body['cursor'], 'cursor-2');
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-2',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-2',
                  'key_numeric': null,
                  'data': {'uid': 'user-2'},
                  'created_at': '2024-01-01T00:00:01Z',
                  'updated_at': '2024-01-01T00:00:01Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': null,
              },
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
      final streamed = <String>[];
      await for (final record in collection.queryStream({'where': {}})) {
        streamed.add(record.data['uid'] as String);
      }

      expect(queryCalls, 2);
      expect(streamed, ['user-1', 'user-2']);
    });

    test('invokes onProgress while streaming documents', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final cursor = queryCalls == 1 ? 'cursor-2' : null;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-$queryCalls',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-$queryCalls',
                  'key_numeric': null,
                  'data': {'uid': 'user-$queryCalls'},
                  'created_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'updated_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': cursor,
              },
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
      final progress = <QueryProgress>[];
      final emitted = <String>[];
      await collection.queryStream(
        {'where': {}},
        pageLimit: 1,
        onProgress: progress.add,
      ).forEach((record) => emitted.add(record.data['uid'] as String));

      expect(queryCalls, 2);
      expect(emitted, ['user-1', 'user-2']);
      expect(progress, hasLength(2));
      expect(progress.first.done, isFalse);
      expect(progress.last.done, isTrue);
      expect(progress.last.nextCursor, isNull);
    });

    test('stops streaming when cancellation token cancels', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          final cursor = queryCalls == 1 ? 'cursor-2' : null;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-$queryCalls',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-$queryCalls',
                  'key_numeric': null,
                  'data': {'uid': 'user-$queryCalls'},
                  'created_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'updated_at': '2024-01-01T00:00:0${queryCalls - 1}Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': cursor,
              },
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
      final source = CancellationTokenSource();
      final progress = <QueryProgress>[];
      final emitted = <String>[];

      await collection.queryStream(
        {'where': {}},
        pageLimit: 1,
        onProgress: progress.add,
        cancellationToken: source.token,
      ).listen((record) {
        emitted.add(record.data['uid'] as String);
        source.cancel();
      }).asFuture<void>();

      expect(queryCalls, 1);
      expect(emitted, ['user-1']);
      expect(progress.every((p) => p.done), isTrue);
      if (progress.isNotEmpty) {
        expect(progress.last.nextCursor, 'cursor-2');
      }
    });
  });

  group('CollectionClient.watch', () {
    test('emits initial snapshot and realtime updates', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-1',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-1',
                  'key_numeric': null,
                  'data': {'uid': 'user-1'},
                  'created_at': '2024-01-01T00:00:00Z',
                  'updated_at': '2024-01-01T00:00:00Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': null,
              },
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        fail('Unexpected request: ${request.method} ${request.url.path}');
      });

      final ws = TestWebSocketChannel();
      var connectorCalls = 0;
      final client = TinyDBClient(
        endpoint: 'https://db.example.com',
        apiKey: 'demo-key',
        httpClient: mock,
        webSocketConnector: (uri, {protocols, headers, pingInterval}) {
          connectorCalls += 1;
          expect(uri.toString(), 'wss://db.example.com/subscribe/users');
          expect(headers?['authorization'], 'Bearer demo-key');
          return ws;
        },
      );

      final collection = await client.collection<JsonMap>('users').sync();

      final queue = StreamQueue(collection.watch());

      final initial = await queue.next.timeout(const Duration(seconds: 5));
      await ws.onListen.timeout(const Duration(seconds: 5));
      ws.addServerMessage(jsonEncode({'ok': true, 'type': 'ack'}));
      final ack = await queue.next.timeout(const Duration(seconds: 5));
      ws.addServerMessage(jsonEncode({
        'type': 'create',
        'id': 'doc-2',
        'data': {'uid': 'user-2'},
        'ts': '2024-01-01T00:00:01Z',
      }));
      final created = await queue.next.timeout(const Duration(seconds: 5));

      await queue.cancel(immediate: true);
      await ws.shutdown();

      expect(queryCalls, 1);
      expect(connectorCalls, 1);
      expect(initial.kind, CollectionWatchEventKind.initial);
      expect(initial.document?.data['uid'], 'user-1');
      expect(ack.kind, CollectionWatchEventKind.ack);
      expect(created.kind, CollectionWatchEventKind.create);
      expect(created.documentId, 'doc-2');
      expect(created.data?['uid'], 'user-2');
    });

    test('cancellation token stops realtime stream after ack', () async {
      var queryCalls = 0;
      final mock = MockClient((request) async {
        if (request.method == 'POST' &&
            request.url.path == '/api/collections') {
          return http.Response(
            jsonEncode(baseCollectionResponse),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        if (request.method == 'POST' &&
            request.url.path == '/api/collections/users/query') {
          queryCalls += 1;
          return http.Response(
            jsonEncode({
              'items': [
                {
                  'id': 'doc-1',
                  'tenant_id': 'tenant-1',
                  'collection_id': 'users',
                  'key': 'user-1',
                  'key_numeric': null,
                  'data': {'uid': 'user-1'},
                  'created_at': '2024-01-01T00:00:00Z',
                  'updated_at': '2024-01-01T00:00:00Z',
                  'deleted_at': null,
                },
              ],
              'pagination': {
                'limit': 1,
                'count': 1,
                'next_cursor': null,
              },
            }),
            200,
            headers: {'content-type': 'application/json'},
          );
        }

        fail('Unexpected request: ${request.method} ${request.url.path}');
      });

      final ws = TestWebSocketChannel();
      var connectorCalls = 0;
      final client = TinyDBClient(
        endpoint: 'https://db.example.com',
        apiKey: 'demo-key',
        httpClient: mock,
        webSocketConnector: (uri, {protocols, headers, pingInterval}) {
          connectorCalls += 1;
          return ws;
        },
      );

      final collection = await client.collection<JsonMap>('users').sync();
      final source = CancellationTokenSource();
      final iterator =
          StreamIterator(collection.watch(cancellationToken: source.token));
      final events = <CollectionWatchEvent<JsonMap>>[];

      expect(await iterator.moveNext().timeout(const Duration(seconds: 5)),
          isTrue);
      events.add(iterator.current);
      expect(events.single.kind, CollectionWatchEventKind.initial);

      await ws.onListen.timeout(const Duration(seconds: 5));

      ws.addServerMessage(jsonEncode({'ok': true, 'type': 'ack'}));
      expect(await iterator.moveNext().timeout(const Duration(seconds: 5)),
          isTrue);
      events.add(iterator.current);
      expect(events.last.kind, CollectionWatchEventKind.ack);

      source.cancel();

      expect(await iterator.moveNext().timeout(const Duration(seconds: 5)),
          isFalse);
      await iterator.cancel();
      await ws.shutdown();

      expect(queryCalls, 1);
      expect(connectorCalls, 1);
      expect(events.map((e) => e.kind),
          [CollectionWatchEventKind.initial, CollectionWatchEventKind.ack]);
    });
  });
}

class TestWebSocketChannel with StreamChannelMixin implements WebSocketChannel {
  TestWebSocketChannel()
      : _outgoing = StreamController<dynamic>(),
        _readyCompleter = Completer<void>() {
    _listenCompleter = Completer<void>();
    _incoming = StreamController<dynamic>.broadcast(onListen: () {
      if (!_listenCompleter.isCompleted) {
        _listenCompleter.complete();
      }
    });
    _sink = _TestWebSocketSink(_outgoing, (code, reason) {
      _closeCode = code;
      _closeReason = reason;
    });
    _readyCompleter.complete();
  }

  late final Completer<void> _listenCompleter;
  late final StreamController<dynamic> _incoming;
  final StreamController<dynamic> _outgoing;
  late final _TestWebSocketSink _sink;
  final Completer<void> _readyCompleter;
  int? _closeCode;
  String? _closeReason;

  Future<void> get onListen => _listenCompleter.future;

  @override
  Stream<dynamic> get stream => _incoming.stream;

  @override
  WebSocketSink get sink => _sink;

  @override
  int? get closeCode => _closeCode;

  @override
  String? get closeReason => _closeReason;

  @override
  String? get protocol => null;

  @override
  Future<void> get ready => _readyCompleter.future;

  void addServerMessage(dynamic message) {
    if (!_incoming.isClosed) {
      _incoming.add(message);
    }
  }

  Future<void> shutdown([int? code, String? reason]) async {
    _closeCode = code ?? _closeCode;
    _closeReason = reason ?? _closeReason;
    if (!_incoming.isClosed) {
      await _incoming.close();
    }
    if (!_outgoing.isClosed) {
      await _outgoing.close();
    }
  }
}

class _TestWebSocketSink implements WebSocketSink {
  _TestWebSocketSink(
    StreamController<dynamic> controller,
    void Function(int? code, String? reason) onClose,
  )   : _controller = controller,
        _sink = controller.sink,
        _onClose = onClose;

  final StreamController<dynamic> _controller;
  final StreamSink<dynamic> _sink;
  final void Function(int? code, String? reason) _onClose;

  @override
  void add(message) => _sink.add(message);

  @override
  void addError(Object error, [StackTrace? stackTrace]) =>
      _sink.addError(error, stackTrace);

  @override
  Future addStream(Stream stream) => _sink.addStream(stream);

  @override
  Future close([int? closeCode, String? closeReason]) async {
    _onClose(closeCode, closeReason);
    if (!_controller.isClosed) {
      await _controller.close();
    }
  }

  @override
  Future get done => _sink.done;
}
