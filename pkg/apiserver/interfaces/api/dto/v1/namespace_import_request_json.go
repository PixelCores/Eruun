package v1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode"
)

// UnmarshalJSON gives namespace import a strict wire contract, including
// nested application mappings. Legacy namespace/includeKinds requests remain
// valid, but unknown and duplicate fields fail closed.
func (r *ImportNamespaceApplicationsRequest) UnmarshalJSON(data []byte) error {
	if r == nil {
		return fmt.Errorf("decode namespace import request into nil target")
	}
	if err := rejectDuplicateNamespaceImportJSONFields(data); err != nil {
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return fmt.Errorf("namespace import request must be a JSON object")
	}

	var decoded ImportNamespaceApplicationsRequest
	seen := make(map[string]struct{}, 6)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return err
		}
		field, ok := token.(string)
		if !ok {
			return fmt.Errorf("namespace import request contains a non-string field name")
		}
		if _, duplicate := seen[field]; duplicate {
			return fmt.Errorf("namespace import request contains duplicate field %q", field)
		}
		seen[field] = struct{}{}

		switch field {
		case "namespace":
			err = decoder.Decode(&decoded.Namespace)
		case "mode":
			err = decoder.Decode(&decoded.Mode)
		case "managementMode":
			err = decoder.Decode(&decoded.ManagementMode)
		case "applications":
			var raw json.RawMessage
			if err = decoder.Decode(&raw); err == nil {
				err = decodeStrictJSON(raw, &decoded.Applications)
			}
		case "planFingerprint":
			err = decoder.Decode(&decoded.PlanFingerprint)
		case "includeKinds":
			err = decoder.Decode(&decoded.IncludeKinds)
		default:
			return fmt.Errorf("json: unknown field %q", field)
		}
		if err != nil {
			return fmt.Errorf("decode namespace import field %q: %w", field, err)
		}
	}
	if _, err = decoder.Token(); err != nil {
		return err
	}
	if err = ensureNamespaceImportJSONEOF(decoder); err != nil {
		return err
	}

	*r = decoded
	return nil
}

func rejectDuplicateNamespaceImportJSONFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := rejectDuplicateNamespaceImportJSONValue(decoder); err != nil {
		return err
	}
	return ensureNamespaceImportJSONEOF(decoder)
}

func rejectDuplicateNamespaceImportJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			field, ok := token.(string)
			if !ok {
				return fmt.Errorf("namespace import request contains a non-string field name")
			}
			folded := foldNamespaceImportJSONField(field)
			if _, duplicate := seen[folded]; duplicate {
				return fmt.Errorf("namespace import request contains duplicate field %q", field)
			}
			seen[folded] = struct{}{}
			if err := rejectDuplicateNamespaceImportJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := rejectDuplicateNamespaceImportJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("namespace import request contains unexpected delimiter %q", delimiter)
	}
	_, err = decoder.Token()
	return err
}

func foldNamespaceImportJSONField(field string) string {
	return strings.Map(func(r rune) rune {
		canonical := r
		for folded := unicode.SimpleFold(r); folded != r; folded = unicode.SimpleFold(folded) {
			if folded < canonical {
				canonical = folded
			}
		}
		return canonical
	}, field)
}

func ensureNamespaceImportJSONEOF(decoder *json.Decoder) error {
	var extra interface{}
	if err := decoder.Decode(&extra); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	return fmt.Errorf("request body contains multiple JSON values")
}
