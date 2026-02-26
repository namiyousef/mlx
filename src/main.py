import base64
import logging
from typing import TypedDict

import polars as pl
from in_n_out_clients.gmail_client import GmailClient
from mcp.server.fastmcp import FastMCP

from config import MAX_EMAIL_RESULTS_PER_CALL
from models import HTTP_PAYLOAD_SCHEMA

logging.basicConfig(
    format="%(levelname)s:%(message)s",
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

mcp = FastMCP("gmail")


class AttachmentMetadata(TypedDict):
    """Type annotation for dictionary that defines a specific attachment from a Gmail
    email."""

    attachment_id: str  # identifier of the attachment
    content_type: str  # the content type of the attachment
    filename: str  # the name of the attachment file


@mcp.tool()
async def get_message_attachments(
    message_id: str, attachments: list[AttachmentMetadata]
) -> str:
    """Get attachments from a particular Gmail email identified by a message id.

    :param message_id: id of the message
    :param attachments: list of attachments to retrieve
    :returns: Success response for the attachments downloadwed
    :rtype: str
    """
    # -- initialise client
    client = GmailClient()
    results = []

    # -- retrieve attachments
    for i, attachment_metadata in enumerate(attachments):
        logger.debug(
            f"Retrieving attachment {i+1}/{len(attachments)}"  # noqa: E226, B950
        )
        file_data = client.get_attachment(
            message_id=message_id, output_dir="/tmp/", **attachment_metadata
        )
        results.append(file_data)

    return "\n---\n".join(results)


@mcp.tool()
async def get_unread_emails() -> str:
    """Get unread emails and return them in a readable format. If unread emails appear
    in a thread, the entire thread is returned so that enough context is provided.

    :returns: Unread emails in a readable format
    """

    # -- initialise client
    client = GmailClient()

    # -- get all unread emails, in batches
    unread_messages = client.list_messages(
        get_all=True, max_results=MAX_EMAIL_RESULTS_PER_CALL
    )

    # -- collect messages in a polars lazyframe for bulk processing
    df_unread_messages = pl.from_records(unread_messages["messages"])

    processed_lfs = []
    for (thread_id,), _ in df_unread_messages.group_by("threadId"):

        # -- retrieve each unread thread in entirety
        unread_threads = client.get_messages_in_thread(thread_id=thread_id)

        lf_thread = (
            pl.from_records(
                unread_threads,
                schema_overrides={"payload": HTTP_PAYLOAD_SCHEMA},
            )
            .lazy()
            .sort("internalDate")
        )

        parts = pl.col("payload").struct.field("parts")
        headers = pl.col("payload").struct.field("headers")
        parts_empty_or_non_exist = (parts.is_null()) | (parts.list.len() == 0)
        mime_type_is_plain = pl.element().struct.field("mimeType") == pl.lit(
            "text/plain"
        )
        attachment_exists = (
            pl.element()
            .struct.field("body")
            .struct.field("attachmentId")
            .is_not_null()
        )
        header_is_content_type = pl.element().struct.field("name") == pl.lit(
            "Content-Type"
        )
        header_is_content_disposition = pl.element().struct.field(
            "name"
        ) == pl.lit("Content-Disposition")

        # -- extract email components from payload
        lf_thread = lf_thread.select(
            pl.col("id"),
            pl.col("threadId"),
            # -- subject
            headers.list.filter(pl.element().struct.field("name") == "Subject")
            .list.first()
            .struct.field("value")
            .alias("subject"),
            # -- from
            headers.list.filter(pl.element().struct.field("name") == "From")
            .list.first()
            .struct.field("value")
            .alias("from"),
            # -- raw email text
            pl.when(parts_empty_or_non_exist)
            .then(pl.col("payload").struct.field("body").struct.field("data"))
            .otherwise(
                parts.list.filter(mime_type_is_plain)
                .list.first()
                .struct.field("body")
                .struct.field("data")
            )
            .alias("data"),
            # -- attachmentId, if exist
            pl.when(~parts_empty_or_non_exist)
            .then(
                pl.col("payload")
                .struct.field("parts")
                .list.filter(attachment_exists)
                .list.eval(
                    pl.element()
                    .struct.field("body")
                    .struct.field("attachmentId")
                )
            )
            .alias("attachment_id"),
            # -- attachment content_type
            pl.when(~parts_empty_or_non_exist)
            .then(
                parts.list.filter(attachment_exists)
                .list.first()
                .struct.field("headers")
                .list.filter(header_is_content_type)
                .list.first()
                .struct.field("value")
            )
            .alias("content_type"),
            # -- attachment content disposition
            pl.when(~parts_empty_or_non_exist)
            .then(
                parts.list.filter(attachment_exists)
                .list.first()
                .struct.field("headers")
                .list.filter(header_is_content_disposition)
                .list.first()
                .struct.field("value")
            )
            .alias("content_disposition"),
        )

        # -- decode respinse message
        lf_thread = lf_thread.with_columns(
            pl.col("data").map_elements(
                lambda s: base64.urlsafe_b64decode(s).decode()
            )
        )

        processed_lfs.append(lf_thread)

    lf_all_threads = pl.concat(processed_lfs)

    records = lf_all_threads.collect().to_dicts()
    # -- make readable
    formatted_records = [f"""
    ITEMS TO REVEAL TO USER:
    Email:
    From: {record['from']}
    Subject: {record['subject']}
    Content: {record['data']}
    ITEMS NOT TO REVEAL TO USER, BUT USEFUL FOR TOOL USE AND
    INTERACTION WITH OTHER TOOLS:
    Thread ID: {record['threadId']}
    Message ID: {record['id']}
    Attachment IDs: {record['attachment_id']}
    Attachment Content-Type: {record['content_type']}
    Attachment Content-Disposition: {record['content_disposition']}
    """ for record in records]
    return "\n---\n".join(formatted_records)


@mcp.tool()
async def create_draft_reply(
    thread_id: str,
    recipients: list[str],
    body: str,
    subject: str,
    cc: list[str] | None = None,
    bcc: list[str] | None = None,
    attachments: list[str] | None = None,
):
    """Creates draft replies in Gmail.

    :param thread_id: thread_id to reply to
    :param recipients: who to send the reply to
    :param body: the text in the email
    :param subject: reply subject
    :param cc: who to cc to, defaults to None
    :param bcc: who to bcc to, defaults to None
    :param attachments: attachments to upload (filepaths), defaults to None
    :return: success response
    """
    client = GmailClient()
    resp = client.reply_to_thread(
        thread_id=thread_id,
        body_text=body,
        to=recipients,
        cc=cc,
        bcc=bcc,
        subject=subject,
        attachments=attachments,
    )
    return str(resp)


if __name__ == "__main__":
    mcp.run(transport="stdio")
