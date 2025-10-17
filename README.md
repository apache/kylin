<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Kylin MCP Server

## Features

### Kylin Tools

* `run_select_query`
  * Execute SQL queries on your Kylin cluster.
  * Input: `sql` (string): The SQL query to execute.

* `list_tables`
  * Call API to get the metadata of a specified Hive table.
  * Input: `project` (string): The name of the project.
  * Input: `database` (string): The name of the database.
  * Input: `table` (string): The name of the table.

### Health Check Endpoint

When running with HTTP or SSE transport, a health check endpoint is available at `/health`. This endpoint:
- Returns `200 OK` with the Kylin version if the server is healthy and can connect to Kylin
- Returns `503 Service Unavailable` if the server cannot connect to Kylin

Example:
```bash
curl http://localhost:8000/health
# Response: Kylin status is UP
```

## Configuration

1. Open the Claude Desktop configuration file located at:
   * On macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   * On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

2. Add the following:

```json
{
  "mcpServers": {
    "kylin-mcp": {
      "command": "uv",
      "args": [
        "run",
        "--with",
        "kylin-mcp",
        "--python",
        "3.10",
        "kylin-mcp"
      ],
      "env": {
        "KYLIN_HOST": "<kylin-host>",
        "KYLIN_PORT": "<kylin-port>",
        "KYLIN_USER": "<kylin-user>",
        "KYLIN_PASSWORD": "<kylin-password>",
        "KYLIN_SECURE": "true",
        "KYLIN_VERIFY": "true",
        "KYLIN_CONNECT_TIMEOUT": "30",
        "KYLIN_SEND_RECEIVE_TIMEOUT": "30"
      }
    }
  }
}
```

3. Locate the command entry for `uv` and replace it with the absolute path to the `uv` executable. This ensures that the correct version of `uv` is used when starting the server. On a mac, you can find this path using `which uv`.

4. Restart Claude Desktop to apply the changes.

### Running Without uv (Using System Python)

If you prefer to use the system Python installation instead of uv, you can install the package from PyPI and run it directly:

1. Install the package using pip:
   ```bash
   python3 -m pip install kylin-mcp
   ```

   To upgrade to the latest version:
   ```bash
   python3 -m pip install --upgrade kylin-mcp
   ```

2. Update your Claude Desktop configuration to use Python directly:

```json
{
  "mcpServers": {
    "kylin-mcp": {
      "command": "python3",
      "args": [
        "-m",
        "kylin_mcp.main"
      ],
      "env": {
        "KYLIN_HOST": "<kylin-host>",
        "KYLIN_PORT": "<kylin-port>",
        "KYLIN_USER": "<kylin-user>",
        "KYLIN_PASSWORD": "<kylin-password>",
        "KYLIN_SECURE": "true",
        "KYLIN_VERIFY": "true",
        "KYLIN_CONNECT_TIMEOUT": "30",
        "KYLIN_SEND_RECEIVE_TIMEOUT": "30"
      }
    }
  }
}
```

Alternatively, you can use the installed script directly:

```json
{
  "mcpServers": {
    "kylin-mcp": {
      "command": "kylin-mcp",
      "env": {
        "KYLIN_HOST": "<kylin-host>",
        "KYLIN_PORT": "<kylin-port>",
        "KYLIN_USER": "<kylin-user>",
        "KYLIN_PASSWORD": "<kylin-password>",
        "KYLIN_SECURE": "true",
        "KYLIN_VERIFY": "true",
        "KYLIN_CONNECT_TIMEOUT": "30",
        "KYLIN_SEND_RECEIVE_TIMEOUT": "30"
      }
    }
  }
}
```

Note: Make sure to use the full path to the Python executable or the `kylin-mcp` script if they are not in your system PATH. You can find the paths using:
- `which python3` for the Python executable
- `which kylin-mcp` for the installed script

## Development

1. In `test-services` directory run `sh docker.sh` to start the Kylin.

2. Add the following variables to a `.env` file in the root of the repository.

*Note: The use of the `default` user in this context is intended solely for local development purposes.*

```bash
KYLIN_HOST=localhost
KYLIN_PORT=7070
KYLIN_USER=admin
KYLIN_PASSWORD=KYLIN
```

3. Run `uv sync` to install the dependencies. To install `uv` follow the instructions [here](https://docs.astral.sh/uv/). Then do `source .venv/bin/activate`.

4. For easy testing with the MCP Inspector, run `fastmcp dev kylin-mcp/mcp_server.py` to start the MCP server.

5. To test with HTTP transport and the health check endpoint:
   ```bash
   # Using default port 8000
   KYLIN_MCP_SERVER_TRANSPORT=http python -m kylin_mcp.main

   # Or with a custom port
   KYLIN_MCP_SERVER_TRANSPORT=http KYLIN_MCP_BIND_PORT=4200 python -m kylin_mcp.main

   # Then in another terminal:
   curl http://localhost:8000/health  # or http://localhost:4200/health for custom port
   ```

### Environment Variables

The following environment variables are used to configure the Kylin connections:

##### Required Variables

* `KYLIN_HOST`: The hostname of your Kylin server
* `KYLIN_USER`: The username for authentication
* `KYLIN_PASSWORD`: The password for authentication

> [!CAUTION]
> It is important to treat your MCP database user as you would any external client connecting to your database, granting only the minimum necessary privileges required for its operation. The use of default or administrative users should be strictly avoided at all times.

##### Optional Variables

* `KYLIN_PORT`: The port number of your ClickHouse server
  * Default: `7070`
  * Usually doesn't need to be set unless using a non-standard port
* `KYLIN_SECURE`: Enable/disable HTTPS connection
  * Default: `"true"`
  * Set to `"false"` for non-secure connections
* `KYLIN_CONNECT_TIMEOUT`: Connection timeout in seconds
  * Default: `"30"`
  * Increase this value if you experience connection timeouts
* `KYLIN_SEND_RECEIVE_TIMEOUT`: Send/receive timeout in seconds
  * Default: `"300"`
  * Increase this value for long-running queries
* `KYLIN_MCP_SERVER_TRANSPORT`: Sets the transport method for the MCP server.
  * Default: `"stdio"`
  * Valid options: `"stdio"`, `"http"`, `"sse"`. This is useful for local development with tools like MCP Inspector.
* `KYLIN_MCP_BIND_HOST`: Host to bind the MCP server to when using HTTP or SSE transport
  * Default: `"127.0.0.1"`
  * Set to `"0.0.0.0"` to bind to all network interfaces (useful for Docker or remote access)
  * Only used when transport is `"http"` or `"sse"`
* `KYLIN_MCP_BIND_PORT`: Port to bind the MCP server to when using HTTP or SSE transport
  * Default: `"8000"`
  * Only used when transport is `"http"` or `"sse"`
