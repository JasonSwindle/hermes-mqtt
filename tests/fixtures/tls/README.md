# TLS test fixtures — TEST ONLY

Throwaway certificates and keys used by [`tests/test_tls.py`](../../test_tls.py) to run a
real TLS broker on loopback. **The private keys in this directory are committed on
purpose and are public. Never use them for anything real.**

| file | purpose |
| --- | --- |
| `ca.pem` | self-signed test CA that signs the server and client certs |
| `server.pem` + `server-key.pem` | broker certificate; SAN `DNS:localhost, IP:127.0.0.1` |
| `client.pem` + `client-key.pem` | mutual-TLS client identity (`clientAuth`) |
| `client-key-encrypted.pem` | the client key encrypted with password `test-key-password`, for encrypted-key tests |

Certificates are valid for ~100 years on purpose, so the suite cannot time-bomb when the
wall clock advances. The CA key is not committed (it is only needed for regeneration).

Regenerate with [`gen-certs.sh`](gen-certs.sh) (requires `openssl`; **not** needed to run
the tests):

```bash
./tests/fixtures/tls/gen-certs.sh
```

These fixtures are only for `127.0.0.1`/`localhost`. A live broker uses its own real
certificate; see the TLS section of the [README](../../../README.md).
