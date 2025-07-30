package confluent

// This package implements a GoFlow2 output format that is compatible with the
// Confluent Schema Registry wire format for Protocol Buffers. When enabled,
// each produced message is prefixed with a magic byte (0), a 4‑byte schema ID
// (big endian) and a list of message indexes encoded as unsigned varints
// (optimised to a single zero byte for the common case of a top‑level message).
// The rest of the payload is a normal Protocol Buffers message encoded using
// `proto.Marshal`. This allows downstream consumers using Confluent tooling
// (for example, Kafka Connect converters or the Confluent clients) to
// automatically resolve schemas without any additional services. The
// application operator must provide the schema ID ahead of time via a command
// line flag; no network calls are made by this format driver.

import (
    "encoding/binary"
    "encoding"
    "flag"
    "fmt"
    "strings"

    "github.com/netsampler/goflow2/v2/format"
    "google.golang.org/protobuf/proto"
)

// confluentDriver implements the FormatDriver interface. The driver
// prepends a Schema Registry header to protobuf messages. The schema ID and
// optional message indexes are supplied via command line flags.
type confluentDriver struct {
    schemaID     uint32
    msgIndexes   []uint64
}

// Flags used to configure the driver. They are package level to allow
// registration via the standard flag package during program initialisation.
var (
    confluentSchemaID   int
    confluentMsgIndexes string
)

// Prepare registers the flags for this format driver. The schema ID is
// required to be non‑zero when using this format; message indexes are
// optional and default to the common case for a top‑level message.
func (d *confluentDriver) Prepare() error {
    flag.IntVar(&confluentSchemaID, "format.confluent.schemaid", 0, "Schema Registry schema ID to include in the Confluent wire header (required)")
    flag.StringVar(&confluentMsgIndexes, "format.confluent.msgindexes", "", "Comma separated list of message indexes for nested Protobuf messages (optional). Defaults to [0] if empty")
    return nil
}

// Init parses the supplied flags and initialises internal state. It is
// invoked automatically by GoFlow2 during program startup.
func (d *confluentDriver) Init() error {
    if confluentSchemaID <= 0 {
        return fmt.Errorf("format confluent requires a positive schema ID via -format.confluent.schemaid")
    }
    d.schemaID = uint32(confluentSchemaID)
    // parse message indexes
    if confluentMsgIndexes == "" {
        // default to the optimised case for a top level message: a single zero
        d.msgIndexes = []uint64{0}
        return nil
    }
    parts := strings.Split(confluentMsgIndexes, ",")
    for _, p := range parts {
        p = strings.TrimSpace(p)
        if p == "" {
            continue
        }
        var idx uint64
        _, err := fmt.Sscanf(p, "%d", &idx)
        if err != nil {
            return fmt.Errorf("invalid message index '%s': %v", p, err)
        }
        d.msgIndexes = append(d.msgIndexes, idx)
    }
    if len(d.msgIndexes) == 0 {
        d.msgIndexes = []uint64{0}
    }
    return nil
}

// encodeIndexes encodes the message indexes according to the Confluent
// specification. If the list consists of a single zero, it returns a single
// zero byte as an optimisation. Otherwise, it encodes the length followed by
// each index using unsigned varint encoding.
func (d *confluentDriver) encodeIndexes() []byte {
    // Optimised case: a single index 0 is encoded as just 0x00
    if len(d.msgIndexes) == 1 && d.msgIndexes[0] == 0 {
        return []byte{0}
    }
    // allocate enough space: worst case is (len+1) * max varint bytes (10) + some for length
    buf := make([]byte, 0, (len(d.msgIndexes)+1)*binary.MaxVarintLen64)
    // encode length
    tmp := make([]byte, binary.MaxVarintLen64)
    n := binary.PutUvarint(tmp, uint64(len(d.msgIndexes)))
    buf = append(buf, tmp[:n]...)
    // encode indexes
    for _, idx := range d.msgIndexes {
        n = binary.PutUvarint(tmp, idx)
        buf = append(buf, tmp[:n]...)
    }
    return buf
}

// Format serialises the provided data as a Confluent wire format Protobuf
// record. It attempts to retrieve the Kafka key (if the message implements
// Key()), then marshals the message using the protobuf library. On success
// it returns the constructed key and value; otherwise, it returns an error.
func (d *confluentDriver) Format(data interface{}) ([]byte, []byte, error) {
    var key []byte
    // extract key if available
    if k, ok := data.(interface{ Key() []byte }); ok {
        key = k.Key()
    }
    // ensure the data can be marshalled as a protobuf message
    msg, ok := data.(proto.Message)
    if !ok {
        // fallback: if the data implements encoding.BinaryMarshaler we call that
        if bm, ok2 := data.(encoding.BinaryMarshaler); ok2 {
            // this will include a length delimiter; use at your own risk
            b, err := bm.MarshalBinary()
            if err != nil {
                return key, nil, err
            }
            // still prepend the schema header
            header := d.headerBytes()
            value := append(header, b...)
            return key, value, nil
        }
        return key, nil, format.ErrNoSerializer
    }
    // marshal message
    pb, err := proto.Marshal(msg)
    if err != nil {
        return key, nil, err
    }
    header := d.headerBytes()
    // assemble final payload
    value := make([]byte, 0, len(header)+len(pb))
    value = append(value, header...)
    value = append(value, pb...)
    return key, value, nil
}

// headerBytes returns the magic byte, schema ID and encoded message indexes.
func (d *confluentDriver) headerBytes() []byte {
    // 1 byte magic + 4 bytes schema ID + message indexes
    idx := d.encodeIndexes()
    header := make([]byte, 1+4+len(idx))
    header[0] = 0
    binary.BigEndian.PutUint32(header[1:5], d.schemaID)
    copy(header[5:], idx)
    return header
}

// init registers the driver with the format registry. Panics on error
// during registration, which only happens when flag registration fails.
func init() {
    d := &confluentDriver{}
    // register under the name "confluent"
    format.RegisterFormatDriver("confluent", d)
}