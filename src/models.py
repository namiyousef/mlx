import polars as pl

# -- polars schemas
# TODO clean this up, making it typing driven from in-n-out-clients
HTTP_HEADER_SCHEMA = pl.Struct(
    [
        pl.Field("name", pl.String),
        pl.Field("value", pl.String),
    ]
)

HTTP_BODY_SCHEMA = pl.Struct(
    [
        pl.Field("size", pl.Int64),
        pl.Field("data", pl.String),  # allow data everywhere
        pl.Field("attachmentId", pl.String),
    ]
)
HTTP_PART_SCHEMA = pl.Struct(
    [
        pl.Field("partId", pl.String),
        pl.Field("mimeType", pl.String),
        pl.Field("filename", pl.String),
        pl.Field("headers", pl.List(HTTP_HEADER_SCHEMA)),
        pl.Field("body", HTTP_BODY_SCHEMA),
    ]
)
HTTP_PAYLOAD_SCHEMA = pl.Struct(
    [
        pl.Field("partId", pl.String),
        pl.Field("mimeType", pl.String),
        pl.Field("filename", pl.String),
        pl.Field("headers", pl.List(HTTP_HEADER_SCHEMA)),
        pl.Field("body", HTTP_BODY_SCHEMA),
        pl.Field("parts", pl.List(HTTP_PART_SCHEMA)),
    ]
)
