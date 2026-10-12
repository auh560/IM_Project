# llfcchat TCP load tester

This tool emulates the real client flow without starting Qt widgets:

1. `POST /user_login` to GateServer.
2. Read `uid`, `token`, ChatServer host and port from the JSON response.
3. Open the persistent TCP connection.
4. Send message `1005` to log in to ChatServer.
5. Send message `1023` heartbeats periodically.
6. Optionally send message `1017` text messages at a fixed per-client rate.
7. Report connection counts, throughput, errors and approximate latency percentiles.

Only run this against systems and databases you own. Use a dedicated test database and Redis instance. Do not pressure-test the verification-code endpoint because it sends real email.

## Build

The tester uses the same Boost headers as the server and otherwise has no third-party runtime dependency.

Visual Studio or a developer command prompt:

```powershell
cmake -S tools/loadtest -B tools/loadtest/build -DBOOST_ROOT=D:/cppsoft/boost_1_83_0
cmake --build tools/loadtest/build --config Release
```

MinGW in the current repository environment:

```powershell
g++ -std=c++17 -O2 -DBOOST_ERROR_CODE_HEADER_ONLY -D_WIN32_WINNT=0x0601 `
  -ID:/cppsoft/boost_1_83_0/include/boost-1_83 `
  tools/loadtest/src/main.cpp -lws2_32 -lmswsock `
  -o tools/loadtest/llfcchat_loadtest.exe
```

## Test data

Create users, friendships and private-chat threads in a dedicated test database. Copy `accounts.example.csv` and provide one row for each virtual user:

```text
email,password,peer_uid,thread_id
```

`password` is the plain ASCII password used by the Qt client. The tester applies the same XOR transformation before calling GateServer. `peer_uid` and `thread_id` are only required when `--message-rate` is greater than zero. Pair two rows by setting each row's `peer_uid` to the other user's uid and using their existing private-chat thread id.

Do not reuse one account for many clients because the project intentionally kicks an existing session when the same uid logs in again.

## Examples

Connection and heartbeat smoke test:

```powershell
tools/loadtest/llfcchat_loadtest.exe `
  --gate-host 127.0.0.1 --gate-port 8080 `
  --accounts tools/loadtest/accounts.csv `
  --connections 100 --duration 60 --ramp 10 `
  --message-rate 0 --threads 4
```

Text-message test at one message per client per second:

```powershell
tools/loadtest/llfcchat_loadtest.exe `
  --gate-host 127.0.0.1 --gate-port 8080 `
  --accounts tools/loadtest/accounts.csv `
  --connections 1000 --duration 300 --ramp 30 `
  --message-rate 1 --payload-size 64 --threads 8
```

Useful options:

```text
--heartbeat 10       Heartbeat interval in seconds.
--timeout 10         HTTP/TCP setup timeout in seconds.
--report-interval 1  Console reporting interval in seconds.
--max-frame 65535    Maximum accepted server response frame.
```

Run `llfcchat_loadtest --help` for the full option list.

## Reading the report

- `active` is the number of clients that completed ChatServer login and are still connected.
- `msg_tx` is the number of text-message requests sent.
- `msg_ack` is the number of text-message acknowledgements received.
- `notify_rx` is the number of peer message notifications received.
- `hb_tx/hb_ack` show heartbeat requests and replies.
- `err` includes HTTP, TCP, protocol and business failures.
- `p50/p95/p99` are approximate acknowledgement latencies based on the request `unique_id`.

If `msg_tx` continues increasing while `msg_ack` stalls, inspect `LogicSystem` queue depth, its single consumer, MySQL latency, Redis latency and synchronous gRPC latency. If `active` drops, inspect heartbeat expiry and connection-cleanup logs.
