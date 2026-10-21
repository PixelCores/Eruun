package grpcapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// decodeTypedRequest converts a generated, field-checked protobuf message into
// the existing domain request DTO. It never invokes an HTTP handler or transport.
func decodeTypedRequest[D any](request proto.Message) (D, error) {
	var result D
	if request == nil {
		return result, fmt.Errorf("request is required")
	}
	if err := validateProtoRequest(request.ProtoReflect()); err != nil {
		return result, err
	}
	data, err := protojson.Marshal(request)
	if err != nil {
		return result, fmt.Errorf("marshal protobuf request: %w", err)
	}
	// ProtoJSON represents 64-bit integers as decimal strings. Existing Go DTOs
	// expect JSON numbers; descriptor-guided conversion leaves actual string
	// fields and dynamic Struct/Value content untouched.
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		return result, fmt.Errorf("decode protobuf JSON: %w", err)
	}
	if err := normalizeProtoJSONNumbers(document, request.ProtoReflect().Descriptor()); err != nil {
		return result, fmt.Errorf("normalize protobuf integers: %w", err)
	}
	data, err = json.Marshal(document)
	if err != nil {
		return result, fmt.Errorf("marshal domain JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("decode domain request: %w", err)
	}
	return result, nil
}

// validateProtoRequest checks wire fields before ProtoJSON can discard unknown
// fields, including deleted traits inside nested component and Job messages.
func validateProtoRequest(message protoreflect.Message) error {
	if len(message.GetUnknown()) != 0 {
		return fmt.Errorf("%s contains unknown fields", message.Descriptor().FullName())
	}
	var err error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			if field.MapValue().Message() != nil {
				value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
					err = validateProtoRequest(item.Message())
					return err == nil
				})
			}
		case field.IsList():
			if field.Message() != nil {
				list := value.List()
				for i := 0; i < list.Len(); i++ {
					if err = validateProtoRequest(list.Get(i).Message()); err != nil {
						break
					}
				}
			}
		case field.Message() != nil:
			err = validateProtoRequest(value.Message())
		}
		return err == nil
	})
	return err
}

func normalizeProtoJSONNumbers(document map[string]any, descriptor protoreflect.MessageDescriptor) error {
	fields := descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		name := field.JSONName()
		value, exists := document[name]
		if !exists || value == nil {
			continue
		}
		if field.IsList() {
			items, ok := value.([]any)
			if !ok {
				return fmt.Errorf("%s is not a list", name)
			}
			for j, item := range items {
				converted, err := normalizeProtoJSONField(item, field.Kind(), field.Message())
				if err != nil {
					return fmt.Errorf("%s[%d]: %w", name, j, err)
				}
				items[j] = converted
			}
			continue
		}
		if field.IsMap() {
			items, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("%s is not a map", name)
			}
			mapValue := field.MapValue()
			for key, item := range items {
				converted, err := normalizeProtoJSONField(item, mapValue.Kind(), mapValue.Message())
				if err != nil {
					return fmt.Errorf("%s[%s]: %w", name, key, err)
				}
				items[key] = converted
			}
			continue
		}
		converted, err := normalizeProtoJSONField(value, field.Kind(), field.Message())
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		document[name] = converted
	}
	return nil
}

func normalizeProtoJSONField(value any, kind protoreflect.Kind, message protoreflect.MessageDescriptor) (any, error) {
	switch kind {
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		valueString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("int64 is not a decimal string")
		}
		if _, err := strconv.ParseInt(valueString, 10, 64); err != nil {
			return nil, err
		}
		return json.Number(valueString), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		valueString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("uint64 is not a decimal string")
		}
		if _, err := strconv.ParseUint(valueString, 10, 64); err != nil {
			return nil, err
		}
		return json.Number(valueString), nil
	case protoreflect.MessageKind:
		if message == nil {
			return value, nil
		}
		if string(message.FullName()) == "google.protobuf.Value" || string(message.FullName()) == "google.protobuf.Struct" ||
			string(message.FullName()) == "google.protobuf.Timestamp" {
			return value, nil
		}
		object, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("message is not an object")
		}
		if err := normalizeProtoJSONNumbers(object, message); err != nil {
			return nil, err
		}
		return object, nil
	default:
		return value, nil
	}
}

// encodeTypedResponse preserves DTO JSON field semantics while producing a
// compile-time concrete protobuf response. Unknown fields fail closed.
func encodeTypedResponse[P proto.Message](value any, target P) (P, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return target, fmt.Errorf("marshal domain response: %w", err)
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, target); err != nil {
		return target, fmt.Errorf("encode protobuf response: %w", err)
	}
	return target, nil
}
