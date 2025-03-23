package main

import (
	"database/sql/driver"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// time.Time format for parsing/formatting input/output
const dateFmt = "2006-01-02Z0700"

// Date defines type to store a date.
type Date struct{ time.Time }

// UnmarshalJSON implements the json.Unmarshaler interface.
func (d *Date) UnmarshalJSON(b []byte) error {
	var s string

	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	*d, err = parseDate(s)
	if err != nil {
		return err
	}

	return nil
}

func parseDate(s string) (Date, error) {
	t, err := time.Parse(dateFmt, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
	}
	return Date{t}, err
}

// MarshalJSON implements the json.Marshaler interface.
func (d Date) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Format(dateFmt))
}

// Scan implements the sql.Scanner interface.
func (d *Date) Scan(value interface{}) error {
	var ok bool
	d.Time, ok = value.(time.Time)
	if !ok {
		return fmt.Errorf("value not type time.Time")
	}

	return nil
}

// Value implements the driver Valuer interface.
func (d Date) Value() (driver.Value, error) {
	return d.Time, nil
}

// EventError defines type to store a error.
type EventError struct {
	StatusCode int
	Data       interface{}
	Message    string
}

// Error implements the error interface.
func (e *EventError) Error() string {
	return e.Message
}

// File defines type to store a file.
type File struct {
	FilePath  string
	CSVReader *csv.Reader
	// copy of file
	io.ReadSeeker
}

// MarshalJSON implements the json.Marshaler interface.
func (f File) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.FilePath)
}

func (f File) Close() error {
	return nil
}

// CSVFileWrapper defines a wrapper to allow reading a file in CSV format.
type CSVFileWrapper File

func (f *CSVFileWrapper) Read() (record []string, err error) {
	if f.CSVReader == nil {
		f.CSVReader = csv.NewReader(f.ReadSeeker)
	}

	return f.CSVReader.Read()
}

// NonStrictInt defines a type to parse non-int into an int value.
type NonStrictInt struct{ int }

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ri *NonStrictInt) UnmarshalJSON(buf []byte) (err error) {
	var i int

	// Attempt to convert string to int.
	if buf[0] == '"' {
		var s string
		if err = json.Unmarshal(buf, &s); err != nil {
			return err
		}

		if s == "" {
			s = "0"
		}

		if i, err = strconv.Atoi(s); err != nil {
			return err
		}
	} else {
		if err = json.Unmarshal(buf, &i); err != nil {
			return err
		}
	}

	*ri = NonStrictInt{i}
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (ri NonStrictInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(ri.int)
}

// NonStrictFloat defines a type to parse non-float into a float value.
type NonStrictFloat struct{ float64 }

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ri *NonStrictFloat) UnmarshalJSON(buf []byte) (err error) {
	var f float64

	// Attempt to convert string to int.
	if buf[0] == '"' {
		var s string
		if err = json.Unmarshal(buf, &s); err != nil {
			return err
		}

		if s == "" {
			s = "0"
		}

		if f, err = strconv.ParseFloat(s, 64); err != nil {
			return err
		}
	} else {
		if err = json.Unmarshal(buf, &f); err != nil {
			return err
		}
	}

	*ri = NonStrictFloat{f}
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (ri NonStrictFloat) MarshalJSON() ([]byte, error) {
	return json.Marshal(ri.float64)
}

// NonStrictBool defines a type to parse non-bool into a bool value.
type NonStrictBool struct{ bool }

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ri *NonStrictBool) UnmarshalJSON(buf []byte) (err error) {
	var b bool
	if b, err = strconv.ParseBool(strings.ToLower(strings.Trim(string(buf), "\""))); err != nil {
		return err
	}

	*ri = NonStrictBool{b}
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (ri NonStrictBool) MarshalJSON() ([]byte, error) {
	return json.Marshal(ri.bool)
}

// NonStrictString defines a type to parse non-string into a string value.
type NonStrictString struct{ string }

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ri *NonStrictString) UnmarshalJSON(buf []byte) (err error) {
	var s string
	if buf[0] == '"' {
		if err = json.Unmarshal(buf, &s); err != nil {
			return err
		}
	} else {
		s = string(buf)
	}

	*ri = NonStrictString{s}
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (ri NonStrictString) MarshalJSON() ([]byte, error) {
	return json.Marshal(ri.string)
}

// SwaggerError defines type to store an error for swagger.
type SwaggerError struct {
	Param   string `json:"param"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

// SwaggerErrorResponse defines type to store a error response for swagger.
type SwaggerErrorResponse struct {
	Data  string       `json:"data"`
	Error SwaggerError `json:"error"`
}

// SwaggerResponse defines type to store a response for swagger.
type SwaggerResponse struct {
	Data interface{} `json:"data"`
}

// RefreshTokenResponse defines type to store a response for refresh token.
type RefreshTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

// NullSwaggerResponse defines type to store a response for swagger with null data.
type NullSwaggerResponse struct {
	Data *string `json:"data" example:"null"`
}

// Thumbnail defines type to store the "thumbnail" system construct.
type Thumbnail struct {
	Width  *int   `json:"width" db:"width" csv:"width"`
	Height *int   `json:"height" db:"height" csv:"height"`
	Url    string `json:"url" db:"url" csv:"url"`
}

// Scan implements sql.Scanner interface.
func (a *Thumbnail) Scan(src interface{}) error {
	switch t := src.(type) {
	case []byte:
		return json.Unmarshal(t, a)
	case nil:
		return nil
	default:
		return fmt.Errorf("cannot convert %T to Thumbnail", src)
	}
}

// Value implements driver.Valuer interface.
func (a Thumbnail) Value() (driver.Value, error) {
	return json.Marshal(a)
}

// Attachment defines type to store the "attachment" system construct.
type Attachment struct {
	Id       string     `json:"id" db:"id" csv:"id"`
	Url      string     `json:"url" db:"url" csv:"url"`
	Filename string     `json:"filename" db:"filename" csv:"filename"`
	Width    *int       `json:"width" db:"width" csv:"width"`
	Height   *int       `json:"height" db:"height" csv:"height"`
	Type     string     `json:"type" db:"type" csv:"type"`
	Size     int        `json:"size" db:"size" csv:"size"`
	Small    *Thumbnail `json:"small" db:"small" csv:"small,inline"`
	Medium   *Thumbnail `json:"medium" db:"medium" csv:"medium,inline"`
	Large    *Thumbnail `json:"large" db:"large" csv:"large,inline"`
	Filepath string     `json:"filepath" db:"filepath" csv:"filepath"`
}

// Scan implements sql.Scanner interface.
func (a *Attachment) Scan(src interface{}) error {
	switch t := src.(type) {
	case []byte:
		return json.Unmarshal(t, a)
	case nil:
		return nil
	default:
		return fmt.Errorf("cannot convert %T to Attachment", src)
	}
}

// Value implements driver.Valuer interface.
func (a Attachment) Value() (driver.Value, error) {
	return json.Marshal(a)
}
