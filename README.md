# Redis Session Store for WSO2 Identity Server

A Redis backed session store, deployed as a single OSGi bundle. It replaces the relational session
storage without any change to the Identity Server, by implementing the two extension points the
framework already provides.

Requires `carbon-identity-framework` on the `feature-pluggable-session-store` branch, which is where
those extension points live.

## Contents

- [How it plugs in](#how-it-plugs-in)
- [Module structure](#module-structure)
- [Data model](#data-model)
- [Building](#building)
- [Configuring](#configuring)
- [Testing with the Identity Server](#testing-with-the-identity-server)
- [Running the unit tests](#running-the-unit-tests)
- [Differences from the relational store](#differences-from-the-relational-store)
- [Limitations of this phase](#limitations-of-this-phase)

## How it plugs in

The framework selects both extension points with the same `SessionStorage.Type` property, so the
session data cannot be split between two stores.

| Extension point | Registered implementation | Covers |
|---|---|---|
| `SessionDataStore` | `RedisSessionDataStore` | The session records, that is `IDN_AUTH_SESSION_STORE` |
| `UserSessionDAO` | `RedisUserSessionDAOImpl` | Session search, session information, user and application mappings, session metadata and federated authentication sessions |

Both are needed. `SessionDataStore` alone would persist sessions to Redis while the session search,
session termination and federated single logout kept reading the database.

`IDN_AUTH_USER`, `IDP` and `SP_APP` stay relational. They are the user, identity provider and
application records rather than session state, they outlive any session, and they are owned by other
components, so this store reads them where they are with `JdbcTemplate`, exactly as the relational
implementation does.

## Module structure

The packages follow the framework, so a class sits where its relational counterpart sits.

```
org.wso2.carbon.identity.session.store.redis
├── config      RedisStoreConfig
├── dao         RedisSessionContext, RedisSessionDataStore, RedisUserSessionDAOImpl,
│               RedisFederatedSessionStore
├── exception   RedisSessionStoreException
├── internal    RedisSessionStoreServiceComponent, RedisSessionStoreDataHolder
├── model       SessionInfo, SessionSearchResult, FederatedSessionRef
└── util        RedisConnectionManager, RedisTemplate, RedisScripts, RedisConstants,
                RedisKeyUtils, RedisValueUtils, SessionFilterEvaluator
```

`RedisSessionContext` is what the store and the DAO share: one connection, one key scheme and the
settings that decide how a key is written. Both are constructed with it, so neither has to expose its
internals to the other and neither has to be built first.

Redis is accessed the way the database is accessed in the framework:

| Relational | Redis |
|---|---|
| `IdentityDatabaseUtil` | `RedisConnectionManager`, which owns the client and the shared connection |
| `JdbcTemplate` | `RedisTemplate`, which executes commands and scripts and translates every failure |
| `SQLQueries` | `RedisScripts`, which holds the Lua scripts |
| `DataAccessException` | `RedisSessionStoreException` |

## Data model

A session is a single hash, so the session and everything belonging to it expire together.

Keys, behind the configurable `key.prefix`, which defaults to `idn`:

```
idn:s:<type>:<sessionId>                   HASH   session record
idn:u:<userId>                             ZSET   sessions of a user, scored by session expiry
idn:tid:<tenantId>                         ZSET   sessions of a tenant, scored by session expiry
idn:fed:<tenantId>:<idpId>:<idpSessionId>  HASH   federated authentication session mapping
idn:fed:idp:<idpSessionId>                 SET    the mappings of an identity provider session index
```

Fields of a session record:

| Field | Holds |
|---|---|
| `t` | Creation time in nanoseconds. Also marks that the session was stored, since a metadata write can create the hash before the session itself |
| `tid` | Tenant id, which may be `-1` when no tenant was available |
| `o` | The serialized session object |
| `u` | The user of the session. It is written only while it is empty, so the first writer wins |
| `m:<property>` | A metadata value, with the property name kept as it is |
| `a:<appId>␟<protocol>␟<subject>` | An application record. Every column of it is part of its primary key, so the field name holds the whole record and the value is empty |
| `f:<tenantId>:<idpId>:<idpSessionId>` | A federated authentication session mapping of this session. The field name holds the key of the mapping, so the value is empty |

Fields of a federated authentication session mapping:

| Field | Holds |
|---|---|
| `sid` | The session context key the mapping belongs to |
| `idp` | Name of the identity provider |
| `auth` | Name of the authenticator |
| `proto` | Inbound authentication protocol of the request |
| `tc` | Creation time in milliseconds |

The session type is part of the key because the framework addresses a record by key and type, and the
identifier spaces overlap: the same `sessionDataKey` is stored under `OAuthSessionDataCache` and under
`AuthenticationContextCache` with different payloads.

The mappings of a session are `f:` fields of the session hash rather than a set of their own, so they
are read in the same call as the session and cannot outlive it.

`idn:fed:idp:<idpSessionId>` holds `<tenantId>:<idpId>` members. It is needed because a back channel
logout carries only the `sid` claim, with no tenant or identity provider to complete the mapping key,
and its members supply the two segments that are missing.

Both sorted sets are scored by the **absolute expiry** of the session, and each expires with the last
session in it. A set scored by creation time cannot be trimmed by expiry, and the normal end of a session
is expiry rather than a logout, so such a set would keep one dead member per session ever created and
grow without bound. Scoring by expiry bounds each index at the sessions that can still be live; the
session search reads the creation time from the sessions themselves and orders on that.

No cluster hash tags are used, as every scripted operation touches a single key.

## Building

```bash
mvn clean install
```

The bundle is at
`org.wso2.carbon.identity.session.store.redis/target/org.wso2.carbon.identity.session.store.redis-1.0.0-SNAPSHOT.jar`.
Lettuce and Netty are embedded in it, so nothing is added to the server classpath.

## Configuring

Everything is configured in `deployment.toml`, under one section. The framework reads the rendered
`identity.xml`, selects the store named there, and hands it the properties of the same section; this
bundle reads the ones it defines and applies a default to every property that is not set.

| Setting | Where | Read by |
|---|---|---|
| `SessionStorage.Type` | `identity.xml`, rendered from `deployment.toml` | The framework, to pick a store |
| `SessionStorage.Properties` | The same element | This bundle, for its connection |

### 1. Deploy the bundle

```bash
cp org.wso2.carbon.identity.session.store.redis/target/org.wso2.carbon.identity.session.store.redis-1.0.0-SNAPSHOT.jar \
   $IS_HOME/repository/components/dropins/
```

### 2. Select the store and configure the connection

Add the following to `$IS_HOME/repository/conf/deployment.toml`.

```toml
[session_storage]
type = "redis"

[session_storage.properties]
hosts = "redis-1:6379"
enable = true
```

That renders the `SessionStorage` element of `identity.xml`:

```xml
<SessionStorage>
    <Type>redis</Type>
    <Properties>
        <Property name="hosts">redis-1:6379</Property>
        <Property name="enable">true</Property>
    </Properties>
</SessionStorage>
```

`Type` is what `SessionMgtUtils.getConfiguredSessionStoreName()` matches against the name this store
registers under, case insensitively, and `Properties` is what
`SessionMgtUtils.getSessionStorageProperties()` returns. Property names are not interpreted by the
framework: they are passed through as they are written, so a store defines its own.

A property name carrying a dot has to be quoted in `deployment.toml`, since an unquoted one is read as
a nested table rather than as a name:

```toml
"connection.timeout.millis" = 2000
```

A host and the flag that turns persistence on are the whole of the minimum configuration. There is
deliberately no default host, so that a store nobody configured cannot connect to whatever Redis
happens to be listening locally, and persistence is off by default, so that a half configured
deployment is never mistaken for a working one. Leaving the host unset aborts the load of the
configuration with

```
The session store is set to redis but no Redis host is configured. Configure 'hosts'.
```

which is logged as an error and leaves the component activated with no store registered, so the server
starts and the failure is read at start-up rather than as a missing session later.

`identity.xml` is generated at every start, and the `SessionStorage` element is rendered by
`identity.xml.j2` of `carbon-identity-framework`, at
`features/identity-core/org.wso2.carbon.identity.core.server.feature/resources/identity.xml.j2`. It is
rendered only when `session_storage.type` is set, so a server that does not configure a store keeps the
JDBC one and the element never appears.

The bundle reads its configuration and registers its store only when `Type` is `redis`. On a server
left on the relational store it activates, logs

```
Redis session store is not selected, as the configured session store is 'jdbc'. The Redis session store is not registered.
```

and does nothing else, so the bundle can stay in `dropins` and switching stores needs only a change to
`deployment.toml`. A store that is registered but disabled connects to nothing, because the connection
is opened on first use and a disabled store issues no operation.

### 3. Available settings

Connection settings:

| Property | Default | Description |
|---|---|---|
| `hosts` | | Required. Comma separated. The server in standalone mode, the sentinels in sentinel mode, the seed nodes in cluster mode. An entry is `host`, `host:port`, `[ipv6]` or `[ipv6]:port`; an IPv6 literal has to be bracketed to carry a port |
| `enable` | `false` | Required. Whether sessions are persisted. When `false`, reads report no session and writes do nothing |
| `mode` | `standalone` | `standalone`, `sentinel` or `cluster` |
| `master.name` | | Required in sentinel mode |
| `username`, `password` | | Credentials of the data nodes |
| `sentinel.password` | | Only when the sentinels need credentials of their own |
| `database` | `0` | Must be `0` in cluster mode, which a clustered node enforces |
| `ssl.enabled` | `false` | On or off only. A truststore, `startTls` and peer verification are not configurable yet |
| `connection.timeout.millis` | `2000` | Also the interval between connection retries |
| `command.timeout.millis` | `1000` | Keep this low, as a session is written on the request thread |
| `key.prefix` | `idn` | Prefix of every key of the store |
| `batch.size` | `250` | Records per pipelined batch when several are read or removed together |

Cluster settings:

| Property | Default | Description |
|---|---|---|
| `cluster.topology.refresh.enabled` | `true` | Whether the topology is refreshed periodically and on a redirect |
| `cluster.topology.refresh.period.seconds` | `60` | |
| `cluster.max.redirects` | `5` | Redirects followed for one command |

Every value is resolved through the secure vault of `identity.xml`, so a password can be a vault alias
rather than a literal, exactly as elsewhere in that file.

### 4. Topology examples

Sentinel:

```toml
[session_storage]
type = "redis"

[session_storage.properties]
mode = "sentinel"
hosts = "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
"master.name" = "mymaster"
password = "redis-password"
enable = true
```

Cluster:

```toml
[session_storage]
type = "redis"

[session_storage.properties]
mode = "cluster"
hosts = "node-1:7000,node-2:7001,node-3:7002"
password = "redis-password"
enable = true
```

## Testing with the Identity Server

### Start Redis

```bash
docker run -d --name redis-is -p 6379:6379 redis:7-alpine
```

For a cluster, `docs/docker-compose.yml` in this repository starts one.

### Start the server and confirm the store is active

```bash
$IS_HOME/bin/wso2server.sh start && tail -f $IS_HOME/repository/logs/wso2carbon.log
```

The log should hold the following. If it does not, the bundle is not in `dropins` or the store type is
not set.

```
Redis session store is activated.
```

### 1. Log in and confirm the session is in Redis

Log in to the My Account application at `https://localhost:9443/myaccount`, then:

```bash
docker exec -it redis-is redis-cli --scan --pattern 'idn:*'
```

You should see a session record, a user index and a tenant index:

```
idn:s:AppAuthFrameworkSessionContextCache:<sessionId>
idn:u:<userId>
idn:tid:-1
```

Inspect the record and confirm it holds an expiry, so that the session is removed by Redis:

```bash
docker exec -it redis-is redis-cli HGETALL idn:s:AppAuthFrameworkSessionContextCache:<sessionId>
docker exec -it redis-is redis-cli TTL idn:s:AppAuthFrameworkSessionContextCache:<sessionId>
```

### 2. Confirm the session search

In My Account, open **Security > Active Sessions**. The session you logged in with should be listed with
its IP address, browser and login time. This exercises the DAO: the tenant index, the metadata fields
and the application fields, plus the application names read from the database.

The same through the REST API:

```bash
curl -k -u admin:admin https://localhost:9443/api/users/v1/me/sessions
```

### 3. Confirm session termination

Terminate the session from **Active Sessions**, or:

```bash
curl -k -X DELETE -u admin:admin \
  https://localhost:9443/api/users/v1/me/sessions/<sessionId>
```

The record and both indexes should be gone:

```bash
docker exec -it redis-is redis-cli --scan --pattern 'idn:*'
```

### 4. Confirm single sign-on across two applications

Register two applications, log in to the first, then open the second. You should not be prompted again,
and the session record should hold two application fields:

```bash
docker exec -it redis-is redis-cli HGETALL idn:s:AppAuthFrameworkSessionContextCache:<sessionId> \
  | grep '^a:'
```

### 5. Confirm federated single logout

Configure an OIDC identity provider with back channel logout, log in through it, and confirm the
mapping exists:

```bash
docker exec -it redis-is redis-cli --scan --pattern 'idn:f*'
```

Trigger a logout at the identity provider. The mapping and its indexes should be removed and the
Identity Server session should end.

### 6. Confirm a Redis outage does not break authentication

```bash
docker stop redis-is
```

Log in again. Authentication should succeed, the log should hold an error for each failed session
operation, and single sign-on should not work while Redis is down, because no session is persisted.
Start Redis again and confirm that persistence resumes without restarting the server:

```bash
docker start redis-is
```

### 7. Confirm cluster mode

With `mode=cluster`, log in and confirm the keys are spread across the nodes:

```bash
docker exec -it <node> redis-cli -c --scan --pattern 'idn:*'
```

Fail over a master and confirm sessions survive and new logins still work, which exercises the topology
refresh.

## Running the unit tests

```bash
mvn clean test
```

The tests need no Redis server. They cover key building, configuration parsing, the connection details
built for each topology, the store models, and the behaviour of the store when Redis cannot be reached.
The behaviour that only a server can show, such as the Lua scripts and the store operations end to end,
is covered by the manual verification above.

## Differences from the relational store

1. **An expired session disappears.** Expiry is the key expiry, so Redis removes the session and its
   information together. The relational store returns an expired record until its cleanup task removes
   it, and callers revalidate the session in any case. This is why the store needs no cleanup task.
2. **A removal deletes the record**, rather than recording a delete operation, so the operation scoped
   reads report the record as absent afterwards. Only the two argument reads have callers in the
   product, and those cannot tell an absent record from a removed one.
3. **A federated mapping expires with its session.** The relational table has no expiry column and no
   periodic task removes its rows, so a federated session that ends by an idle timeout is left behind
   until a stored procedure runs.
4. **A federated mapping is stored in one operation**, because its key is its uniqueness constraint, so
   the check and update of the relational store cannot conflict.
5. **The applications of a session are ordered** by application id, which the relational query only
   specified on one database.
6. **`validateLastOperationOnSessionData(key, type, "DELETE")` is always `false`.** A removal deletes the
   record rather than appending a delete row, so a delete can never be the last operation of a record that
   is still there. Nothing in `carbon-identity-framework` asks this question with a delete; an inbound
   authentication connector that does gets the answer the relational store gives once its cleanup task has
   removed the delete row. Grep `wso2-extensions` for callers before release.

The parsing of a session search result is not reimplemented. The framework parser is reused, so a
subject holding a `:` or a `|` behaves exactly as it does with the relational store, including where
that behaviour is wrong.

## Limitations of this phase

- **The temporary session store is not implemented.** `IDN_AUTH_SESSION_TEMP` and the settings of it are
  out of scope, so every record is held in the one keyspace.
- **Filtering of the sessions that belong to a tenant is not supported.** `getSessions(tenantId, filter,
  limit, sortOrder)` throws `UnsupportedOperationException`, so the administrative session listing of a
  tenant cannot be served from this store yet.
