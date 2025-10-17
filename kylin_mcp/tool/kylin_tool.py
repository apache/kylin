# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Apache Kylin MCP Tools Manager
Responsible for tool registration, management, scheduling and routing, does not contain specific business logic implementation
"""

import logging
import concurrent.futures
import atexit

from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.tools import Tool
from fastmcp.exceptions import ToolError
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from kylin_mcp.utils import kylin_instance
from kylin_mcp.utils.config import get_config
from kylin_mcp.tool.kylin_prompt import KYLIN_PROMPT

MCP_SERVER_NAME = "mcp-kylin"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(MCP_SERVER_NAME)

QUERY_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=10)
atexit.register(lambda: QUERY_EXECUTOR.shutdown(wait=True))
SELECT_QUERY_TIMEOUT_SECS = 30

load_dotenv()

mcp = FastMCP(name=MCP_SERVER_NAME)


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> PlainTextResponse:
    """Health check endpoint for monitoring server status.

    Returns OK if the server is running and can connect to Kylin.
    """
    try:
        # Try to create a client connection to verify Kylin connectivity
        client = create_kylin_client()
        response = client.check_ke_health()
        return PlainTextResponse(f"Kylin status is {response['status']}")
    except Exception as e:
        # Return 503 Service Unavailable if we can't connect to Kylin
        return PlainTextResponse(
            f"ERROR - Cannot connect to Kylin: {str(e)}", status_code=503
        )


def list_project():
    """List available Kylin projects."""
    logger.info(f"Listing Kylin projects")
    client = create_kylin_client()
    return client.get_project()


def list_tables(
    project: str,
    database: str = "",
    table: str = "",
    is_fuzzy: bool = False,
    extension: bool = True,
    page_offset: int = 0,
    page_size: int = 10000,
    user_session: bool = False,
):
    """List available Kylin tables in a database, including schema, comment, row count, and column count."""
    logger.info(
        f"Listing tables in project '{project}', database '{database}', table '{table}', "
        f"is_fuzzy={is_fuzzy}, extension={extension}, "
        f"page_offset={page_offset}, page_size={page_size}, "
        f"user_session={user_session}"
    )
    client = create_kylin_client()

    return client.list_tables(
        project_name=project,
        database=database,
        table=table,
        is_fuzzy=is_fuzzy,
        extension=extension,
        page_offset=page_offset,
        page_size=page_size,
        user_session=user_session,
    )


def execute_query(project: str, sql: str, offset: int = 0, limit: int = 0):
    client = create_kylin_client()
    try:
        res = client.execute_query(project, sql, offset=offset, limit=limit)
        logger.info(f"Query returned {len(res.get('results'))} rows")
        return {"columns": res.get("columnMetas"), "rows": res.get("results")}
    except Exception as err:
        logger.error(f"Error executing query: {err}")
        raise ToolError(f"Query execution failed: {str(err)}")


def run_select_query(project: str, sql: str, offset: int = 0, limit: int = 0):
    """Run a SELECT query in a Kylin database"""
    logger.info(
        f"Executing SELECT query: {sql} in project: {project}, offset: {offset}, limit: {limit}"
    )
    try:
        future = QUERY_EXECUTOR.submit(
            execute_query, project, sql, offset=offset, limit=limit
        )
        try:
            result = future.result(timeout=SELECT_QUERY_TIMEOUT_SECS)
            # Check if we received an error structure from execute_query
            if isinstance(result, dict) and "error" in result:
                logger.warning(f"Query failed: {result['error']}")
                # MCP requires structured responses; string error messages can cause
                # serialization issues leading to BrokenResourceError
                return {
                    "status": "error",
                    "message": f"Query failed: {result['error']}",
                }
            return result
        except concurrent.futures.TimeoutError:
            logger.warning(
                f"Query timed out after {SELECT_QUERY_TIMEOUT_SECS} seconds: {query}"
            )
            future.cancel()
            raise ToolError(
                f"Query timed out after {SELECT_QUERY_TIMEOUT_SECS} seconds"
            )
    except ToolError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in run_select_query: {str(e)}")
        raise RuntimeError(f"Unexpected error during query execution: {str(e)}")


def create_kylin_client():
    client_config = get_config().get_client_config()
    logger.info(
        f"Creating Kylin client connection to {client_config['host']}:{client_config['port']} "
        f"as {client_config['username']} "
        f"(secure={client_config['secure']}, verify={client_config['verify']}, "
        f"connect_timeout={client_config['connect_timeout']}s, "
        f"send_receive_timeout={client_config['send_receive_timeout']}s)"
    )

    try:
        # create RestClient instance
        client = kylin_instance.connect(
            host=client_config["host"],
            port=client_config["port"],
            username=client_config["username"],
            password=client_config["password"],
        )
        logger.info(f"Successfully connected to Kylin server")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to Kylin: {str(e)}")
        raise


# Register tools based on configuration
mcp.add_tool(Tool.from_function(list_project))
mcp.add_tool(Tool.from_function(list_tables))
mcp.add_tool(Tool.from_function(run_select_query))
logger.info("Kylin tools registered")
