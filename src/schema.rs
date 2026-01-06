use duckdb::core::{LogicalTypeHandle, LogicalTypeId};
use warc::WarcHeader;

#[derive(Debug)]
pub struct Field {
    pub name: &'static str,
    pub header: WarcHeader,
    pub field_type: LogicalTypeId,
}

impl Field {
    pub fn get_field_type_handle(&self) -> LogicalTypeHandle {
        match self.field_type {
            LogicalTypeId::Varchar => LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeId::Integer => LogicalTypeHandle::from(LogicalTypeId::Integer),
            LogicalTypeId::Timestamp => LogicalTypeHandle::from(LogicalTypeId::Timestamp),
            _ => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        }
    }
}

pub static WARC_FIELDS: &[Field] = &[
    Field {
        name: "record_id",
        header: WarcHeader::RecordID,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "content_length",
        header: WarcHeader::ContentLength,
        field_type: LogicalTypeId::Integer,
    },
    Field {
        name: "date",
        header: WarcHeader::Date,
        field_type: LogicalTypeId::Timestamp,
    },
    Field {
        name: "type",
        header: WarcHeader::WarcType,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "content_type",
        header: WarcHeader::ContentType,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "concurrent_to",
        header: WarcHeader::ConcurrentTo,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "block_digest",
        header: WarcHeader::BlockDigest,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "payload_digest",
        header: WarcHeader::PayloadDigest,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "ip_address",
        header: WarcHeader::IPAddress,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "refers_to",
        header: WarcHeader::RefersTo,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "target_uri",
        header: WarcHeader::TargetURI,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "truncated",
        header: WarcHeader::Truncated,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "warcinfo_id",
        header: WarcHeader::WarcInfoID,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "filename",
        header: WarcHeader::Filename,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "profile",
        header: WarcHeader::Profile,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "identified_payload_type",
        header: WarcHeader::IdentifiedPayloadType,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "segment_number",
        header: WarcHeader::SegmentNumber,
        field_type: LogicalTypeId::Integer,
    },
    Field {
        name: "segment_origin_id",
        header: WarcHeader::SegmentOriginID,
        field_type: LogicalTypeId::Varchar,
    },
    Field {
        name: "segment_total_length",
        header: WarcHeader::SegmentTotalLength,
        field_type: LogicalTypeId::Integer,
    },
];
