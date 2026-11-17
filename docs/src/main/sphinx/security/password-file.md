# Password file authentication

Trino can be configured to enable frontend password authentication over
HTTPS for clients, such as the CLI, or the JDBC and ODBC drivers. The
username and password are validated against usernames and passwords stored
in a file.

Password file authentication is very similar to {doc}`ldap`. Please see
the LDAP documentation for generic instructions on configuring the server
and clients to use TLS and authenticate with a username and password.

Using {doc}`TLS <tls>` and {doc}`a configured shared secret
</security/internal-communication>` is required for password file
authentication.

## Password authenticator configuration

To enable password file authentication, set the {doc}`password authentication
type <authentication-types>` in `etc/config.properties`:

```properties
http-server.authentication.type=PASSWORD
```

In addition, create a `etc/password-authenticator.properties` file on the
coordinator with the `file` authenticator name:

```text
password-authenticator.name=file
file.password-file=/path/to/password.db
```

The following configuration properties are available:

| Property                         | Description                                                       |
| -------------------------------- | ----------------------------------------------------------------- |
| `file.password-file`             | Path of the password file.                                        |
| `file.refresh-period`            | How often to reload the password file. Defaults to `5s`.          |
| `file.auth-token-cache.max-size` | Maximum number of cached verification results. Defaults to `1000`. |
| `file.auth-metadata-enabled` | Read versioned authentication metadata. Defaults to `false`. |

### Verification cache

The file authenticator caches Boolean verification results.
Each key contains the username plus a complete HMAC-SHA256 identifier.
The cache does not retain plaintext passwords.

Each loaded password store owns a random HMAC key.
Reloading the password file replaces that store.
The verification cache does not change the password file format.

## Password files

### File format

The password file contains a list of usernames and passwords, one per line,
separated by a colon. Passwords must be securely hashed using bcrypt or PBKDF2.

bcrypt passwords start with `$2y$` and must use a minimum cost of `8`:

```text
test:$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa
```

PBKDF2 passwords are composed of the iteration count, followed by the
hex encoded salt and hash:

```text
test:1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0
```

### Managed authentication metadata

The default-off implementation remains under review.
Its contract authorizes no deployment or per-login traffic.
The `file.auth-metadata-enabled` property defaults to `false`.
The reader reserves metadata syntax only when that property is enabled.
Otherwise, it preserves the existing generic username and file behavior.

The opt-in format places a header before any nonempty credential row:

```text
# trufflehog-auth:1
# trufflehog-target {"principal":"example-db.alice","backend":"hoglake"}
example-db.alice:<hash>
# trufflehog-target {"principal":"legacy-db.root","backend":"ducklake"}
legacy-db.root:<hash>
# trufflehog-operator {"principal":"__trino_ops"}
__trino_ops:<hash>
```

The hashes above are placeholders.
Actual rows still use the existing bcrypt or PBKDF2 format.
The reader splits each credential row at its first colon.
It preserves colons inside PBKDF2 hashes.
Metadata does not change username validation.

Each target record identifies an exact credential principal.
It requires exactly one recognized backend: `hoglake` or `ducklake`.
The operator record identifies only `__trino_ops`.
It supplies operator evidence instead of tenant backend evidence.
The reader rejects duplicate, conflicting, orphan, malformed, or unsupported metadata.
It also rejects a target record for the operator.

A legacy file without metadata remains supported.
Its credentials have UNKNOWN classification.
UNKNOWN never means DuckLake.
When {doc}`managed acquisition <caller-tokens>` is enabled, UNKNOWN terminates authentication.
Merely enabling the file reader does not enable acquisition.

The authenticator captures one password store for each authentication attempt.
That store supplies both password verification and classification.
The loaded revision fingerprints the complete raw file, including metadata.
It requires no backend groups or separate inventory file.

Old readers reject the new metadata lines.
Reader support must precede approved publication.
The existing admission barrier must acknowledge the complete artifact.
These prerequisites authorize no rollout.
A stale complete snapshot does not provide immediate revocation.

### Creating a password file

Password files utilizing the bcrypt format can be created using the
[htpasswd](https://httpd.apache.org/docs/current/programs/htpasswd.html)
utility from the [Apache HTTP Server](https://httpd.apache.org/).
The cost must be specified, as Trino enforces a higher minimum cost
than the default.

Create an empty password file to get started:

```text
touch password.db
```

Add or update the password for the user `test`:

```text
htpasswd -B -C 10 password.db test
```

(verify-authentication)=
### Verify configuration

To verify password file authentication, log in to the {doc}`Web UI
</admin/web-interface>`, and connect with the Trino {doc}`CLI </client/cli>` to
the cluster:

- Connect to the Web UI from your browser using a URL that uses HTTPS, such as
  `https://trino.example.com:8443`. Enter a username in the `Username` text
  box and the corresponding password in the `Password` text box, and log in to
  the UI. Confirm that you are not able to log in using an incorrect username
  and password combination. A successful login displays the username in the
  top right corner of the UI.
- Connect with the Trino CLI using a URL that uses HTTPS, such as
  `https://trino.example.net:8443` with the addition of the `--user` and
  `--password` properties:

```text
./trino --server https://trino.example.com:8443 --user test --password
```

The above command quotes you for a password. Supply the password set for the
user entered for the `--user` property to use the `trino>` prompt. Successful
authentication allows you to run queries from the CLI.

To test the connection, send a query:

```text
trino> SELECT 'rocks' AS trino;

trino
-------
rocks
(1 row)

Query 20220919_113804_00017_54qfi, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.12 [0 rows, 0B] [0 rows/s, 0B/s]
```

## Reserved user names

The optional `file.reserved-user-regex` property rejects matching user names before
password lookup, including cached passwords. Use it when another authenticator owns
a credential namespace. Leave it unset to preserve the default behavior.

For example, when enabling Duckgres service credentials alongside persistent users,
reserve service logins in the file authenticator:

```properties
file.reserved-user-regex=(?:[^.]+[.])?svc_[0-9a-f]{24}
```
