import base64
from typing import Any
import polars as pl
import httpx
import json
from mcp.server.fastmcp import FastMCP
from in_n_out_clients.gmail_client import GmailClient

mcp = FastMCP("gmail")

# TODO add logger. Wuestion: will logging interfere with MCP?

@mcp.tool()
async def get_unread_emails():
    """Function that gets unread emails from the user's Gmail account.
    Iteration one: very simply, we assume that each message will get a reply, even if they appear from the same thread
    """

    client = GmailClient()
    unread_messages = client.list_messages(get_all=True, max_results=50)

    lf = pl.from_records(unread_messages["messages"])

    header_schema = pl.Struct([
        pl.Field("name", pl.String),
        pl.Field("value", pl.String),
    ])

    body_schema = pl.Struct([
        pl.Field("size", pl.Int64),
        pl.Field("data", pl.String),  # allow data everywhere
    ])
    part_schema = pl.Struct([
        pl.Field("partId", pl.String),
        pl.Field("mimeType", pl.String),
        pl.Field("filename", pl.String),
        pl.Field("headers", pl.List(header_schema)),
        pl.Field("body", body_schema),
    ])
    payload_schema = pl.Struct([
        pl.Field("partId", pl.String),
        pl.Field("mimeType", pl.String),
        pl.Field("filename", pl.String),
        pl.Field("headers", pl.List(header_schema)),
        pl.Field("body", body_schema),
        pl.Field("parts", pl.List(part_schema)),
    ])

    decoded_frames = []
    for (thread_id,), _ in lf.group_by("threadId"):
        unread_threads = client.get_messages_in_thread(thread_id=thread_id)

        messages_lf = pl.from_records(unread_threads, schema_overrides={"payload": payload_schema})
        parts_col = pl.col("payload").struct.field("parts")
        messages_lf = messages_lf.select(
            pl.col("payload").struct.field("headers").list.filter(pl.element().struct.field("name") == "Subject").list.first().struct.field("value").alias("subject"),
            pl.col("payload").struct.field("headers").list.filter(pl.element().struct.field("name") == "From").list.first().struct.field("value").alias("from"),
            pl.col("id"),
            pl.col("threadId"),
            pl.when(
                (parts_col.is_null()) | (parts_col.list.len() == 0)
            )
            .then(
                pl.col("payload").struct.field("body").struct.field("data")
            )
            .otherwise(
                parts_col.list.filter(pl.element().struct.field("mimeType") == "text/plain").list.first().struct.field("body").struct.field("data")
                # parts_col.list.first().struct.field("body").struct.field("data")

            )
            .alias("data")
        )

        # TODO add support for large messages, e.g. when the data comes as an attachment not as a payload
        # TODO can improve logic by using polars native decode ... need conversion from url safe encoding
        # TODO add test sanitisation?
        messages_lf = messages_lf.with_columns(
            pl.col("data").map_elements(lambda s: base64.urlsafe_b64decode(s).decode())
        )

        decoded_frames.append(messages_lf)

    lf = pl.concat(decoded_frames)
    records = lf.to_dicts()
    emails = [
    f"""
    Email:
    From: {record['from']}
    Subject: {record['subject']}
    Content: {record['data']}
    (Don't show user: Thread ID: {record['threadId']}, Message ID: {record['id']})
    """ for record in records
    ]
    return "\n---\n".join(emails)

@mcp.tool()
async def create_draft_reply(
   thread_id: str, recipients: list[str],
   body: str,
   subject: str,
   cc: list[str] = None,
   bcc: list[str] = None,
   ):
    client = GmailClient()
    resp = client.reply_to_thread(
        thread_id=thread_id,
        body_text=body,
        to=recipients,
        cc=cc,
        bcc=bcc,
        subject=subject,
    )
    return str(resp)


if __name__ == "__main__":
    mcp.run(transport="stdio")