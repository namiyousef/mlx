import base64
from typing import Any
import polars as pl
import httpx
import json
from mcp.server.fastmcp import FastMCP
from in_n_out_clients.gmail_client import GmailClient
from typing import TypedDict

mcp = FastMCP("gmail")

# TODO add logger. Wuestion: will logging interfere with MCP?

class AttachmentMetadata(TypedDict):
    attachment_id: str
    content_type: str
    filename: str

@mcp.tool()
async def get_message_attachments(
    message_id: str, attachments: list[AttachmentMetadata]
):

    client = GmailClient()
    results = []
    for attachment_metadata in attachments:
        file_data = client.get_attachment(message_id=message_id, output_dir="/tmp/", **attachment_metadata)
        results.append(file_data)
    return "\n---\n".join(results)   

    # attachments_to_read = [
    # f"""
    # Attachment ID: {attachment_id}
    # Data: {data}
    # """ for attachment_id, data in decoded_files.items()
    # ]
    # return "\n---\n".join(attachments_to_read)   

@mcp.tool()
async def get_unread_emails() -> str:
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
        pl.Field("attachmentId", pl.String),
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
        # DO we filter the thread by it's context?

        messages_lf = pl.from_records(unread_threads, schema_overrides={"payload": payload_schema}).sort("internalDate")
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
            )
            .alias("data"),
            pl.when(
                parts_col.list.len() > 0
            ).then(
                pl.col("payload").struct.field("parts").list.filter(pl.element().struct.field("body").struct.field("attachmentId").is_not_null()).list.eval(pl.element().struct.field("body").struct.field("attachmentId"))
            ).alias("attachmentId"),
            pl.when(
                parts_col.list.len() > 0
            ).then(
                pl.col("payload").struct.field("parts").list.filter(
                    pl.element().struct.field("body").struct.field("attachmentId").is_not_null()
                ).list.first().struct.field("headers").list.filter(
                    pl.element().struct.field("name") == pl.lit("Content-Type")
                ).list.first().struct.field("value")
            ).alias("content_type"),
            pl.when(
                parts_col.list.len() > 0
            ).then(
                pl.col("payload").struct.field("parts").list.filter(
                    pl.element().struct.field("body").struct.field("attachmentId").is_not_null()
                ).list.first().struct.field("headers").list.filter(
                    pl.element().struct.field("name") == pl.lit("Content-Disposition")
                ).list.first().struct.field("value")
            ).alias("content_disposition")
            # pl.when(
            #     (parts_col.is_null()) | (parts_col.list.len() == 0)
            # ).then(
            #     pl.col("payload").struct.field("body").struct.field("attachmentId").str.split('')
            # ).otherwise(
            #     pl.col("payload").struct.field("parts").list.filter(pl.element().struct.field("body").struct.field("attachmentId").is_not_null()).list.eval(pl.element().struct.field("body").struct.field("attachmentId"))
            # ),
        )

        

        # messages_lf = messages_lf.with_columns(
        #     pl.col("body").struct.field("data").alias("data"),
        #     pl.col("body").struct.field("attachmendId").alias("attachmentId"),
        # )
        # print(messages_lf)
        # raise Exception()

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
    ITEMS TO REVEAL TO USER:
    Email:
    From: {record['from']}
    Subject: {record['subject']}
    Content: {record['data']}
    ITEMS NOT TO REVEAL TO USER, BUT USEFUL FOR TOOL USE AND INTERACTION WITH OTHER TOOLS:
    Thread ID: {record['threadId']}
    Message ID: {record['id']}
    Attachment IDs: {record['attachmentId']}
    Attachment Content-Type: {record['content_type']}
    Attachment Content-Disposition: {record['content_disposition']}
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

# if __name__ == "__main__":
#     get_unread_emails()
#     # get_message_attachments(message_id="19c9143edb373644", attachment_ids=["ANGjdJ_ffDykIqOtKpcika2RbMa4J5QgcTE8VNEpcXz_8gha-Ag5OY5hp7YnAwxN9YhUX4dZ3uf4y4Hwzl85ulEch4PqnQLz3uKA_FHZ5ZNQ_Us4JLciEsX07NCQ1bRpEIQGQ1CtK9aeoaroxW1rXcq7ObFvbVhFNVlN82DksYD2DIXPVIKyOFuxXp0vUwZzhmEEesKK2uWs__XNW6Ae3g1nCi7vM64cfTZvsup2DLMS55drnr0bK-uVC4OdsYLWjLloQQcF0t3gdB21wCJC5e33N68SaQXHiLLhyQI2KXhL3c7yOv3bC0pkrF5RPR4wjX2WTGq6q_6h6a3g0AU01e99JfSa4mg-Ik4QSHZZfnTFVZBESvq3xQDreKFS1LlRMxyVhM0Kr7L_AucvndZR"])
#     # get_message_attachments(message_id="19c9676339c57cb8", attachment_ids=["ANGjdJ_1QRWsLGNMiCnNAQsUsunREPpdWh8zjBNdAyWBFpVRJzuKTa2hElcuELPwh3DDGfF2ov6Usm_hlDc4U2l07IFib3nJE5qtW7eaqFVnRHkFjBDFx9yLLR4MpvroxdQ06TYWnc8TIxHcr4wbAjPgtBTaKEWI02pbe7TIz0LiJL7VGlx1gr-zHqjOxz0TuLS58kX0xssxjUVVxCaG9sEBlm3FJIZ3f8lZxiv7l_ggycAH0I1TYauYBCiXD12kDb2r9GpZWAHQG8c_m9pmZYuME2j7Nx1g1fan-B9qgiJ7ayJupBnp6wpchtGMS66MZnurixhyWWT2BOJKl3JHWwLkWuPIVD8ZUUlqE_brAT-faAhggMH9-mBWN8HSnQ4DKTXOXNmY1SMwBq92HiIF"])